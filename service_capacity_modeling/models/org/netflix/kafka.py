import logging
import math
from enum import Enum
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

from pydantic import BaseModel
from pydantic import Field

from service_capacity_modeling.interface import AccessConsistency
from service_capacity_modeling.interface import AccessPattern
from service_capacity_modeling.interface import Buffer
from service_capacity_modeling.interface import BufferComponent
from service_capacity_modeling.interface import Buffers
from service_capacity_modeling.interface import CapacityDesires
from service_capacity_modeling.interface import CapacityPlan
from service_capacity_modeling.interface import CapacityRequirement
from service_capacity_modeling.interface import certain_float
from service_capacity_modeling.interface import certain_int
from service_capacity_modeling.interface import Clusters
from service_capacity_modeling.interface import Consistency
from service_capacity_modeling.interface import CurrentZoneClusterCapacity
from service_capacity_modeling.interface import DataShape
from service_capacity_modeling.interface import Drive
from service_capacity_modeling.interface import GIB_IN_BYTES
from service_capacity_modeling.interface import GlobalConsistency
from service_capacity_modeling.interface import Instance
from service_capacity_modeling.interface import Interval
from service_capacity_modeling.interface import MEGABIT_IN_BYTES
from service_capacity_modeling.interface import MIB_IN_BYTES
from service_capacity_modeling.interface import QueryPattern
from service_capacity_modeling.interface import RegionContext
from service_capacity_modeling.interface import Requirements
from service_capacity_modeling.models import CapacityModel
from service_capacity_modeling.models import utils
from service_capacity_modeling.models.common import compute_stateful_zone
from service_capacity_modeling.models.common import normalize_cores
from service_capacity_modeling.models.common import sqrt_staffed_cores
from service_capacity_modeling.models.common import zonal_requirements_from_current
from service_capacity_modeling.models.org.netflix.iso_date_math import iso_to_seconds

logger = logging.getLogger(__name__)


class ClusterType(str, Enum):
    strong = "strong"
    ha = "high-availability"


def _get_current_zonal_cluster(
    desires: CapacityDesires,
) -> Optional[CurrentZoneClusterCapacity]:
    return (
        None
        if desires.current_clusters is None
        else (
            desires.current_clusters.zonal[0]
            if len(desires.current_clusters.zonal)
            else None
        )
    )


def _is_same_instance_family(cluster, target_family):
    """Check if cluster has a different instance family than the target."""
    if not cluster or not cluster.cluster_instance:
        return False
    return cluster.cluster_instance.family == target_family


def _estimate_kafka_requirement(  # pylint: disable=too-many-positional-arguments
    instance: Instance,
    desires: CapacityDesires,
    copies_per_region: int,
    hot_retention_seconds: float,
    zones_per_region: int = 3,
    required_zone_size: Optional[int] = None,
) -> Tuple[CapacityRequirement, Tuple[str, ...]]:
    """Estimate the capacity required for one zone given a regional desire

    The input desires should be the **regional** desire, and this function will
    return the zonal capacity requirement
    """
    normalized_to_mib = desires.model_copy(deep=True)
    read_mib_per_second = (
        normalized_to_mib.query_pattern.estimated_mean_read_size_bytes.mid
        / MIB_IN_BYTES
    )
    write_mib_per_second = (
        normalized_to_mib.query_pattern.estimated_mean_write_size_bytes.mid
        / MIB_IN_BYTES
    )

    # 1 concurrent reader, cpu time is per MiB read
    # 0.5 MiB / second
    rps = normalized_to_mib.query_pattern.estimated_read_per_second
    wps = normalized_to_mib.query_pattern.estimated_write_per_second
    normalized_to_mib.query_pattern.estimated_read_per_second = rps.scale(
        read_mib_per_second
    )
    normalized_to_mib.query_pattern.estimated_write_per_second = wps.scale(
        write_mib_per_second
    )
    # use the current cluster capacity if available
    current_zonal_capacity = _get_current_zonal_cluster(desires)

    bw_in = (
        (write_mib_per_second * MIB_IN_BYTES) * copies_per_region
    ) / MEGABIT_IN_BYTES
    bw_out = (
        (
            (read_mib_per_second * MIB_IN_BYTES)
            + ((write_mib_per_second * MIB_IN_BYTES) * (copies_per_region - 1))
        )
    ) / MEGABIT_IN_BYTES
    if (
        current_zonal_capacity
        and current_zonal_capacity.cluster_instance
        and required_zone_size is not None
        and desires.current_clusters is not None
    ):
        # zonal_requirements_from_current uses the midpoint utilization of the
        # current cluster. For Kafka, we want to use the high value instead
        # for cpu, disk, network, etc.
        normalize_midpoint_desires = desires.model_copy(deep=True)
        # This is checked in the if statement above. Assert here for mypy linter purpose
        assert normalize_midpoint_desires.current_clusters is not None
        curr_disk = desires.current_clusters.zonal[0].disk_utilization_gib
        curr_cpu = desires.current_clusters.zonal[0].cpu_utilization
        curr_network = desires.current_clusters.zonal[0].network_utilization_mbps
        normalize_midpoint_desires.current_clusters.zonal[
            0
        ].disk_utilization_gib = Interval(
            low=curr_disk.high, mid=curr_disk.high, high=curr_disk.high
        )
        normalize_midpoint_desires.current_clusters.zonal[0].cpu_utilization = Interval(
            low=curr_cpu.high, mid=curr_cpu.high, high=curr_cpu.high
        )
        normalize_midpoint_desires.current_clusters.zonal[
            0
        ].network_utilization_mbps = Interval(
            low=curr_network.high, mid=curr_network.high, high=curr_network.high
        )
        capacity_requirement = zonal_requirements_from_current(
            current_cluster=normalize_midpoint_desires.current_clusters,
            buffers=desires.buffers,
            instance=instance,
            reference_shape=current_zonal_capacity.cluster_instance,
        )
        needed_cores = int(capacity_requirement.cpu_cores.mid)
        needed_disk = int(capacity_requirement.disk_gib.mid)
        needed_network_mbps = int(capacity_requirement.network_mbps.mid)
        logger.debug(
            "kafka normalized needed cores: %s", capacity_requirement.cpu_cores
        )
    else:
        # We have no existing utilization to go from
        needed_cores = (
            normalize_cores(
                core_count=sqrt_staffed_cores(normalized_to_mib),
                target_shape=instance,
                reference_shape=desires.reference_shape,
            )
            // zones_per_region
        )

        # (Nick): Keep 40% of available bandwidth for node recovery
        # (Joey): For kafka BW = BW_write + BW_reads
        #   let X = input write BW
        #   BW_in = X * RF
        #   BW_out = X * (consumers) + X * (RF - 1)
        #   BW = (in + out) because duplex then 40% headroom.
        needed_network_mbps = int((max(bw_in, bw_out) * 1.40) // zones_per_region)

        # NOTE: data_shape is region, we need to convert it to zonal
        # If we don't have an existing cluster, the estimated state size should be
        # at most 40% of the total cluster's available disk.
        # i.e. needed_disk = state_size * 2.5
        needed_disk = math.ceil(
            (desires.data_shape.estimated_state_size_gib.mid // zones_per_region) * 2.5,
        )

    # Keep the last N seconds hot in cache
    needed_memory = (
        (write_mib_per_second * hot_retention_seconds) // 1024
    ) // zones_per_region

    logger.debug(
        "Need (instance, cpu, mem, disk) = (%s, %s, %s, %s)",
        instance.name,
        needed_cores,
        needed_memory,
        needed_disk,
    )

    return (
        CapacityRequirement(
            requirement_type="kafka-zonal",
            reference_shape=desires.reference_shape,
            cpu_cores=certain_int(needed_cores),
            mem_gib=certain_float(needed_memory),
            disk_gib=certain_float(needed_disk),
            network_mbps=certain_float(needed_network_mbps),
            context={
                "bw_in_mbps": bw_in,
                "bw_out_mbps": bw_out,
                "hot_retention_seconds": hot_retention_seconds,
                "replication_factor": copies_per_region,
            },
        ),
        ("spend", "disk", "mem"),
    )


def _upsert_params(cluster, params):
    if cluster.cluster_params:
        cluster.cluster_params.update(params)
    else:
        cluster.cluster_params = params


def _kafka_read_io(rps, io_size_kib, size_gib, recovery_seconds: int) -> float:
    # Get enough disk read IO capacity for some reads
    # In practice we have cache reducing this by 99% or more
    read_ios = rps * 0.05
    # Recover the node in 60 minutes, to do that we need
    # Estimate that we are using ~ 50% of the disk prior to a recovery
    size_kib = size_gib * 0.5 * (1024 * 1024)
    recovery_ios = max(1, size_kib / io_size_kib) / recovery_seconds
    # Leave 50% headroom for read IOs since generally we will hit cache
    return (read_ios + int(round(recovery_ios))) * 1.5


# pylint: disable=too-many-locals
# pylint: disable=too-many-return-statements
# pylint: disable=too-many-positional-arguments
def _estimate_kafka_cluster_zonal(  # noqa: C901
    instance: Instance,
    drive: Drive,
    desires: CapacityDesires,
    hot_retention_seconds,
    zones_per_region: int = 3,
    copies_per_region: int = 2,
    require_local_disks: bool = False,
    require_attached_disks: bool = False,
    required_zone_size: Optional[int] = None,
    max_regional_size: int = 150,
    max_local_disk_gib: int = 1024 * 5,
    min_instance_cpu: int = 2,
    min_instance_memory_gib: int = 12,
    require_same_instance_family: bool = True,
) -> Optional[CapacityPlan]:

    # Kafka doesn't like to deploy on single CPU instances or with < 12 GiB of ram
    if instance.cpu < min_instance_cpu or instance.ram_gib < min_instance_memory_gib:
        return None

    # if we're not allowed to use attached disks, skip EBS only types
    if instance.drive is None and require_local_disks:
        return None

    # if we're not allowed to use local disks, skip ephems
    if instance.drive is not None and require_attached_disks:
        return None

    # Kafka only deploys on gp3 drives right now
    if instance.drive is None and drive.name != "gp3":
        return None

    # If there is a current cluster, check if we are restricted to same instance family
    current_zonal_cluster = _get_current_zonal_cluster(desires)
    if (
        current_zonal_cluster
        and require_same_instance_family
        and not _is_same_instance_family(current_zonal_cluster, instance.family)
    ):
        return None

    requirement, regrets = _estimate_kafka_requirement(
        instance=instance,
        desires=desires,
        zones_per_region=zones_per_region,
        copies_per_region=copies_per_region,
        hot_retention_seconds=hot_retention_seconds,
        required_zone_size=required_zone_size,
    )

    # Account for sidecars and base system memory
    base_mem = (
        desires.data_shape.reserved_instance_app_mem_gib
        + desires.data_shape.reserved_instance_system_mem_gib
    )

    # Kafka clusters in prod (tier 0+1) need at least 2 nodes per zone
    min_count = 1
    if desires.service_tier < 2:
        min_count = 2

    # Kafka read io / second is zonal
    normalized_to_mib = desires.model_copy(deep=True)
    read_mib_per_second: int = (
        int(normalized_to_mib.query_pattern.estimated_mean_read_size_bytes.mid)
        // MIB_IN_BYTES
        // zones_per_region
    )
    write_mib_per_second: int = (
        int(normalized_to_mib.query_pattern.estimated_mean_write_size_bytes.mid)
        // MIB_IN_BYTES
        // zones_per_region
    )

    # All Kafka IOs are sequential, so they can use the group size
    read_ios_per_second = max(1, (read_mib_per_second * 1024) // drive.seq_io_size_kib)
    write_ios_per_second = max(
        1, (write_mib_per_second * 1024) // drive.seq_io_size_kib
    )
    max_attached_disk_gib = 8 * 1024

    cluster = compute_stateful_zone(
        instance=instance,
        drive=drive,
        needed_cores=int(requirement.cpu_cores.mid),
        needed_disk_gib=int(requirement.disk_gib.mid),
        needed_memory_gib=int(requirement.mem_gib.mid),
        needed_network_mbps=requirement.network_mbps.mid,
        # Leave 2x overhead for both reads and writes.
        # Readers are sequential, 100MiB/s read is 100Mib / block_size ios
        # Writers are sequential, 100MiB/s write is 100MiB / block_size ios
        required_disk_ios=lambda size, count: (
            _kafka_read_io(
                rps=read_ios_per_second / count,
                # Kafka does sequential IO
                io_size_kib=drive.seq_io_size_kib,
                size_gib=size,
                # Enough IO to recover a node in 60 minutes
                recovery_seconds=60 * 60,
            ),
            # Leave 100% IO headroom for writes
            copies_per_region * (write_ios_per_second / count) * 2,
        ),
        # Disk buffer is already added when computing kafka disk requirements
        required_disk_space=lambda x: x,
        max_local_disk_gib=max_local_disk_gib,
        cluster_size=lambda x: x,
        min_count=max(min_count, required_zone_size or 1),
        # Sidecars and Variable OS Memory
        # Kafka currently uses 8GiB fixed, might want to change to min(30, x // 2)
        reserve_memory=lambda instance_mem_gib: base_mem + 8,
        # allow up to 8TiB of attached EBS
        max_attached_disk_gib=max_attached_disk_gib,
    )

    # Communicate to the actual provision that if we want reduced RF
    params = {"kafka.copies": copies_per_region}
    _upsert_params(cluster, params)

    # This is roughly the disk we would have tried to provision with the current
    # cluster's instance count (or required_zone_size)
    if required_zone_size is not None:
        space_gib = max(1, math.ceil(requirement.disk_gib.mid / required_zone_size))
        ebs_gib = utils.next_n(space_gib, n=100)

        # Max allowed disk size in `compute_stateful_zone`
        if instance.drive is not None and instance.drive.size_gib > 0:
            max_size = min(max_local_disk_gib, instance.drive.size_gib)
        elif max_attached_disk_gib is not None:
            max_size = max_attached_disk_gib
        else:
            max_size = drive.max_size_gib / 3

        # Capacity planner only allows ~ 5TB disk (max_size) for gp3 drives
        # or max_attached_disk_gib if provided.
        # If ebs_gib > max_size, we do not have enough instances within the
        # required_zone_size for the required disk. In these cases, it is
        # not possible for cluster.count == required_zone_size. We should
        # allow higher instance count for these cases so that we return some result
        # If we did not exceed the max disk size with the required_zone_size, then
        # we only allow topologies that match the desired zone size
        if ebs_gib <= max_size and cluster.count != required_zone_size:
            return None

    # Kafka clusters generally should try to stay under some total number
    # of nodes. Orgs do this for all kinds of reasons such as
    #   * Security group limits. Since you must have < 500 rules if you're
    #       ingressing public ips)
    #   * Maintenance. If your restart script does one node at a time you want
    #       smaller clusters so your restarts don't take months.
    #   * NxN network issues. Sometimes smaller clusters of bigger nodes
    #       are better for network propagation
    if cluster.count > (max_regional_size // zones_per_region):
        return None

    ec2_cost = zones_per_region * cluster.annual_cost

    # Account for the clusters and replication costs
    kafka_costs = {"kafka.zonal-clusters": ec2_cost}

    cluster.cluster_type = "kafka"
    clusters = Clusters(
        annual_costs=kafka_costs,
        zonal=[cluster] * zones_per_region,
        regional=[],
        services=[],
    )

    return CapacityPlan(
        requirements=Requirements(
            zonal=[requirement] * zones_per_region, regrets=regrets
        ),
        candidate_clusters=clusters,
    )


class NflxKafkaArguments(BaseModel):
    copies_per_region: int = Field(
        default=2,
        description=(
            "How many copies of the data will exist e.g. RF=3. If not supplied"
            " this will be determined from mode"
        ),
    )
    cluster_type: ClusterType = Field(
        default=ClusterType.ha,
        description="If the cluster is 'strong' consistency or 'high availability'",
    )
    retention: str = Field(
        default="PT8H", description="How long to retain data in this cluster."
    )
    hot_retention: str = Field(
        default="PT10M",
        description=(
            "How long to retain data in page cache for consumers to skip IO. "
            "Typically consumers lag under 10s, but ensure up to 10M can be handled"
        ),
    )
    require_local_disks: bool = Field(
        default=False,
        description="If local (ephemeral) drives are required",
    )
    require_attached_disks: bool = Field(
        default=False,
        description="If attached (ebs) drives are required",
    )
    required_zonal_size: Optional[int] = Field(
        default=None,
        description="Require zonal clusters to be this size (force vertical scaling)",
    )
    max_regional_size: int = Field(
        default=150,
        description="What is the maximum size of a cluster in this region",
    )
    max_local_disk_gib: int = Field(
        default=5120,
        description=(
            "The maximum amount of data we store per node. Used to limit "
            "recovery duration on failure."
        ),
    )
    min_instance_cpu: int = Field(
        default=2,
        description="The minimum number of instance CPU to allow",
    )
    min_instance_memory_gib: int = Field(
        default=12,
        description="The minimum amount of instance memory to allow",
    )


class NflxKafkaCapacityModel(CapacityModel):

    HA_DEFAULT_REPLICATION_FACTOR = 2
    SC_DEFAULT_REPLICATION_FACTOR = 3

    @staticmethod
    def capacity_plan(
        instance: Instance,
        drive: Drive,
        context: RegionContext,
        desires: CapacityDesires,
        extra_model_arguments: Dict[str, Any],
    ) -> Optional[CapacityPlan]:
        cluster_type: ClusterType = ClusterType(
            extra_model_arguments.get("cluster_type", "high-availability")
        )
        replication_factor = NflxKafkaCapacityModel.HA_DEFAULT_REPLICATION_FACTOR
        if cluster_type == ClusterType.strong:
            replication_factor = NflxKafkaCapacityModel.SC_DEFAULT_REPLICATION_FACTOR
        copies_per_region: int = extra_model_arguments.get(
            "copies_per_region", replication_factor
        )
        if cluster_type == ClusterType.strong and copies_per_region < 3:
            raise ValueError("Strong consistency and RF<3 doesn't work")

        max_regional_size: int = extra_model_arguments.get("max_regional_size", 150)
        # Very large nodes are hard to cache warm
        max_local_disk_gib: int = extra_model_arguments.get(
            "max_local_disk_gib", 1024 * 5
        )
        min_instance_cpu: int = extra_model_arguments.get("min_instance_cpu", 2)
        min_instance_memory_gib: int = extra_model_arguments.get(
            "min_instance_memory_gib", 12
        )
        hot_retention_seconds: float = iso_to_seconds(
            extra_model_arguments.get("hot_retention", "PT10M")
        )
        require_local_disks: bool = extra_model_arguments.get(
            "require_local_disks", False
        )
        require_attached_disks: bool = extra_model_arguments.get(
            "require_attached_disks", False
        )
        required_zone_size: Optional[int] = extra_model_arguments.get(
            "required_zone_size", None
        )
        # By default, for existing clusters, restrict to only using same instance family
        require_same_instance_family: bool = extra_model_arguments.get(
            "require_same_instance_family", True
        )

        return _estimate_kafka_cluster_zonal(
            instance=instance,
            drive=drive,
            desires=desires,
            zones_per_region=context.zones_in_region,
            copies_per_region=copies_per_region,
            require_local_disks=require_local_disks,
            require_attached_disks=require_attached_disks,
            required_zone_size=required_zone_size,
            max_regional_size=max_regional_size,
            max_local_disk_gib=max_local_disk_gib,
            min_instance_cpu=min_instance_cpu,
            min_instance_memory_gib=min_instance_memory_gib,
            hot_retention_seconds=hot_retention_seconds,
            require_same_instance_family=require_same_instance_family,
        )

    @staticmethod
    def description():
        return "Netflix Streaming Kafka Model"

    @staticmethod
    def extra_model_arguments_schema() -> Dict[str, Any]:
        return NflxKafkaArguments.model_json_schema()

    @staticmethod
    def allowed_cloud_drives() -> Tuple[Optional[str], ...]:
        # Kafka at Netflix only supports 3rd gen SSD storage
        return ("gp3",)

    @staticmethod
    def default_desires(user_desires, extra_model_arguments: Dict[str, Any]):
        # Default to 10MiB/s and a single reader
        concurrent_readers = max(
            1, int(user_desires.query_pattern.estimated_read_per_second.mid)
        )
        query_pattern = user_desires.query_pattern.model_dump()
        if "estimated_mean_write_size_bytes" in query_pattern:
            write_bytes = Interval(**query_pattern["estimated_mean_write_size_bytes"])
        else:
            write_bytes = certain_int(10 * 1024 * 1024)

        read_bytes = write_bytes.scale(concurrent_readers)
        retention = extra_model_arguments.get("retention", "PT8H")
        retention_secs = iso_to_seconds(retention)

        # write throughput * retention * replication factor = usage
        replication_factor = NflxKafkaCapacityModel.HA_DEFAULT_REPLICATION_FACTOR
        if extra_model_arguments.get("cluster_type", None) == ClusterType.strong:
            replication_factor = NflxKafkaCapacityModel.SC_DEFAULT_REPLICATION_FACTOR
        state_gib = (
            write_bytes.mid * retention_secs * replication_factor
        ) / GIB_IN_BYTES

        # By supplying these buffers we can deconstruct observed utilization into
        # load versus buffer.
        compute_buffer_ratio = 2.5 if user_desires.service_tier in (0, 1) else 2.0
        buffers = Buffers(
            default=Buffer(ratio=1.5),
            desired={
                # Amount of compute buffer that we need to reserve in addition to
                # cpu_headroom_target that is reserved on a per instance basis
                "compute": Buffer(
                    ratio=compute_buffer_ratio, components=[BufferComponent.compute]
                ),
                # This makes sure we use only 40% of the available storage
                "storage": Buffer(ratio=2.5, components=[BufferComponent.storage]),
            },
        )

        return CapacityDesires(
            query_pattern=QueryPattern(
                access_pattern=AccessPattern.throughput,
                access_consistency=GlobalConsistency(
                    same_region=Consistency(
                        target_consistency=AccessConsistency.read_your_writes
                    ),
                    cross_region=Consistency(
                        target_consistency=AccessConsistency.never
                    ),
                ),
                estimated_mean_write_size_bytes=write_bytes,
                estimated_mean_read_size_bytes=read_bytes,
                # How much on-cpu time to read or write 1 MiB
                # 42 MiB / second in 3 CPU seconds = 14 MiB / CPU second
                # This = 1000 / 14 = 71 millisecond / MiB
                # To read 1 MiB costs 71 millisecond of CPU time
                # Later tuned down based on training data
                estimated_mean_read_latency_ms=Interval(
                    low=20, mid=40, high=75, confidence=0.98
                ),
                # Wild-Ass-Guess that writes are slightly cheaper
                # Because no disks/context switching??
                estimated_mean_write_latency_ms=Interval(
                    low=20, mid=30, high=75, confidence=0.98
                ),
            ),
            data_shape=DataShape(
                estimated_state_size_gib=Interval(
                    low=state_gib * 0.5,
                    mid=state_gib,
                    high=state_gib * 2,
                    confidence=0.98,
                ),
                # Gandalf, system stuffs
                reserved_instance_app_mem_gib=1,
                # Connection overhead, kernel, etc ...
                reserved_instance_system_mem_gib=3,
            ),
            buffers=buffers,
        )


nflx_kafka_capacity_model = NflxKafkaCapacityModel()
