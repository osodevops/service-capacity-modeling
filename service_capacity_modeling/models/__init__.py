from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Tuple

from service_capacity_modeling.interface import AccessConsistency
from service_capacity_modeling.interface import AccessPattern
from service_capacity_modeling.interface import CapacityDesires
from service_capacity_modeling.interface import CapacityPlan
from service_capacity_modeling.interface import CapacityRegretParameters
from service_capacity_modeling.interface import certain_float
from service_capacity_modeling.interface import DataShape
from service_capacity_modeling.interface import Drive
from service_capacity_modeling.interface import FixedInterval
from service_capacity_modeling.interface import Instance
from service_capacity_modeling.interface import QueryPattern
from service_capacity_modeling.interface import RegionContext


class CapacityModel:
    """Stateless interface for defining a capacity model

    To define a capacity model you must implement two pure functions.

    The first, `capacity_plan` method calculates the best possible
    CapacityPlan (cluster layout) given a concrete `Instance` shape (cpu,
    memory, disk etc ...) and attachable `Drive` (e.g. cloud drives like GP2)
    along with a concrete instead of CapacityDesires (qps, latency, etc ...).

    The second, `regret` method calculates a relative regret (higher is worse)
    of two capacity plans. For example if the optimal plan computed by
    `capacity_plan` would allocate `100` CPUs and the proposed plan only
    allocates `50` we regret that choice as we are under-provisioning.

    """

    def __init__(self):
        pass

    @staticmethod
    def capacity_plan(
        instance: Instance,
        drive: Drive,
        context: RegionContext,
        desires: CapacityDesires,
        **kwargs,
    ) -> Optional[CapacityPlan]:
        """Given a concrete hardware shape and desires, return a candidate

        This is the only required method on this interface. Your model
        must either:
            * Return None to indicate there is no viable use of the
              instance/drive which meets the user's desires
            * Return a CapapacityPlan containing the model's calculation
              of how much CPU/RAM/disk etc ... that is required and
        """
        # quiet pylint
        (_, _, _, _) = (instance, drive, context, desires)
        return None

    @staticmethod
    def regret(
        regret_params: CapacityRegretParameters,
        optimal_plan: CapacityPlan,
        proposed_plan: CapacityPlan,
    ) -> Dict[str, float]:
        """Optional cost model for how much we regret a choice

        After the capacity planner has simulated a bunch of possible outcomes
        We need to evaluate each cluster against the optimal choice for a
        given requirement.

        Our default model of cost is just related to money and disk footprint:

        Under an assumption that cost is a reasonable single dimension
        to compare to clusters on, we penalize under-provisioned (cheap)
        clusters more than expensive ones.

        :return: A componentized regret function where the total regret is
        the sum of all componenets. This is not just a single number so as you
        develop more complex regret functions you can debug why clusters are
        or are not being chosen
        """
        optimal_cost = float(optimal_plan.candidate_clusters.total_annual_cost)
        plan_cost = float(proposed_plan.candidate_clusters.total_annual_cost)

        if plan_cost >= optimal_cost:
            cost_regret = (
                (plan_cost - optimal_cost) * regret_params.over_provision_cost
            ) ** regret_params.cost_exponent
        else:
            cost_regret = (
                (optimal_cost - plan_cost) * regret_params.under_provision_cost
            ) ** regret_params.cost_exponent

        optimal_disk = sum(req.disk_gib.mid for req in optimal_plan.requirements.zonal)
        optimal_disk += sum(
            req.disk_gib.mid for req in optimal_plan.requirements.regional
        )

        plan_disk = sum(req.disk_gib.mid for req in proposed_plan.requirements.zonal)
        plan_disk += sum(
            req.disk_gib.mid for req in proposed_plan.requirements.regional
        )

        # We regret not having the disk space for a dataset
        if optimal_disk > plan_disk:
            disk_regret = (
                (optimal_disk - plan_disk) * regret_params.under_provision_cost
            ) ** regret_params.disk_exponent
        else:
            disk_regret = 0

        return {
            "cost": round(cost_regret, 4),
            "disk_space": round(disk_regret, 4),
        }

    @staticmethod
    def description() -> str:
        """ Optional description of the model """
        return "No description"

    @staticmethod
    def extra_model_arguments() -> Sequence[Tuple[str, str, str]]:
        """Optional list of extra keyword arguments

        Some models might take additional arguments to capacity_plan.
        They can convey that context to callers here along with a
        description of each argument

        Result is a sequence of (name, type = default, description) pairs
        For example:
            (
                ("arg_name", "int = 3", "my custom arg"),
            )
        """
        return tuple()

    @staticmethod
    def compose_with(user_desires: CapacityDesires, **kwargs) -> Tuple[str, ...]:
        """Return additional model names to compose with this one

        Often used for dependencies.
        """
        # quiet pylint
        _ = user_desires
        return tuple()

    @staticmethod
    def default_desires(user_desires: CapacityDesires, **kwargs):
        """Optional defaults to apply given a user desires

        Often users do not know what the on-cpu time of their queries
        will be, but the models often have a good idea. For example
        databases usually have some range of on-cpu time for point queries
        (latency access) versus throughput.

        This is also a good place to throw ValueError on AccessPattern
        or AccessConsistency that cannot be met.
        """
        unlikely_consistency_models = (
            AccessConsistency.linearizable,
            AccessConsistency.serializable,
        )
        if user_desires.query_pattern.access_consistency in unlikely_consistency_models:
            raise ValueError(
                f"Most services do not support {unlikely_consistency_models}"
            )

        if user_desires.query_pattern.access_pattern == AccessPattern.latency:
            return CapacityDesires(
                query_pattern=QueryPattern(
                    access_pattern=AccessPattern.latency,
                    estimated_mean_read_latency_ms=certain_float(1),
                    estimated_mean_write_latency_ms=certain_float(1),
                    # "Single digit milliseconds"
                    read_latency_slo_ms=FixedInterval(
                        low=0.4, mid=4, high=10, confidence=0.98
                    ),
                    write_latency_slo_ms=FixedInterval(
                        low=0.4, mid=4, high=10, confidence=0.98
                    ),
                ),
                data_shape=DataShape(),
            )
        else:
            return CapacityDesires(
                query_pattern=QueryPattern(
                    access_pattern=AccessPattern.throughput,
                    estimated_mean_read_latency_ms=certain_float(10),
                    estimated_mean_write_latency_ms=certain_float(20),
                    # "Tens of milliseconds"
                    read_latency_slo_ms=FixedInterval(
                        low=10, mid=50, high=100, confidence=0.98
                    ),
                    write_latency_slo_ms=FixedInterval(
                        low=10, mid=50, high=100, confidence=0.98
                    ),
                )
            )
