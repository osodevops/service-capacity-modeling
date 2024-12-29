import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional
from pydantic import BaseModel
from enum import Enum

# Import the necessary components from the original script
from service_capacity_modeling.models.org.netflix.kafka import (
    ClusterType,
    NflxKafkaArguments,
    nflx_kafka_capacity_model,
    Instance,
    Drive,
    CapacityDesires,
    RegionContext,
    QueryPattern,
    DataShape,
    Interval,
    AccessPattern,
    GlobalConsistency,
    Consistency,
    AccessConsistency,
)

st.title("Netflix Kafka Capacity Modeling Tool")

def create_instance(cpu: int, ram_gib: float) -> Instance:
    """Create an Instance object with basic parameters"""
    return Instance(
        name="custom-instance",
        cpu=cpu,
        cpu_ghz=2.5,
        net_mbps=1000.0,
        ram_gib=ram_gib,
        network_mbps=10000,  # Example value
        drive=None,  # Using attached drives
        cost_per_hour=1.0,  # Example value
    )

def create_drive() -> Drive:
    """Create a Drive object with gp3 parameters"""
    return Drive(
        name="gp3",
        cost_per_gib_month=0.08,  # Example GP3 cost
        seq_io_size_kib=1024,
        max_size_gib=16384,
        max_read_ios=10000,
        max_write_ios=10000,
    )

# Sidebar for configuration
st.sidebar.header("Cluster Configuration")

# Basic cluster settings
cluster_type = st.sidebar.selectbox(
    "Cluster Type",
    options=[ClusterType.ha, ClusterType.strong],
    format_func=lambda x: "High Availability" if x == ClusterType.ha else "Strong Consistency"
)

copies_per_region = st.sidebar.number_input(
    "Copies per Region",
    min_value=2,
    max_value=5,
    value=3 if cluster_type == ClusterType.strong else 2
)

# Instance configuration
st.sidebar.subheader("Instance Configuration")
instance_cpu = st.sidebar.number_input("CPU Cores", min_value=2, value=4)
instance_memory = st.sidebar.number_input("Memory (GiB)", min_value=12, value=32)

# Workload configuration
st.header("Workload Configuration")
col1, col2 = st.columns(2)

with col1:
    write_size_mb = st.number_input(
        "Write Size (MiB)",
        min_value=1,
        value=10,
        help="Average size of write operations in MiB"
    )
    writes_per_second = st.number_input(
        "Writes per Second",
        min_value=1,
        value=100,
        help="Number of write operations per second"
    )

with col2:
    retention_hours = st.number_input(
        "Data Retention (hours)",
        min_value=1,
        value=8,
        help="How long to retain data"
    )
    hot_retention_minutes = st.number_input(
        "Hot Data Retention (minutes)",
        min_value=1,
        value=10,
        help="How long to retain data in page cache"
    )

# Create model arguments
model_args = {
    "cluster_type": cluster_type,
    "copies_per_region": copies_per_region,
    "retention": f"PT{retention_hours}H",
    "hot_retention": f"PT{hot_retention_minutes}M",
    "require_local_disks": False,
    "require_attached_disks": True,
}

# Create the instance and drive objects
instance = create_instance(instance_cpu, instance_memory)
drive = create_drive()

# Create the capacity desires
desires = CapacityDesires(
    query_pattern=QueryPattern(
        access_pattern=AccessPattern.throughput,
        access_consistency=GlobalConsistency(
            same_region=Consistency(target_consistency=AccessConsistency.read_your_writes),
            cross_region=Consistency(target_consistency=AccessConsistency.never),
        ),
        estimated_mean_write_size_bytes=Interval(
            low=write_size_mb * 1024 * 1024 * 0.8,
            mid=write_size_mb * 1024 * 1024,
            high=write_size_mb * 1024 * 1024 * 1.2,
            confidence=0.98
        ),
        estimated_write_per_second=Interval(
            low=writes_per_second * 0.8,
            mid=writes_per_second,
            high=writes_per_second * 1.2,
            confidence=0.98
        ),
        estimated_mean_read_latency_ms=Interval(low=20, mid=40, high=75, confidence=0.98),
        estimated_mean_write_latency_ms=Interval(low=20, mid=30, high=75, confidence=0.98),
    ),
    data_shape=DataShape(
        estimated_state_size_gib=Interval(
            low=100,
            mid=200,
            high=400,
            confidence=0.98
        ),
        reserved_instance_app_mem_gib=1,
        reserved_instance_system_mem_gib=3,
    )
)

# Create context
context = RegionContext(
    region="us-east-1",
    zones_in_region=3,
)

if st.button("Calculate Capacity Plan"):
    try:
        # Get capacity plan
        plan = nflx_kafka_capacity_model.capacity_plan(
            instance=instance,
            drive=drive,
            context=context,
            desires=desires,
            extra_model_arguments=model_args,
        )

        if plan:
            st.success("Capacity plan generated successfully!")
            
            # Display requirements
            st.header("Zonal Requirements")
            if plan.requirements.zonal:
                req = plan.requirements.zonal[0]
                st.write({
                    "CPU Cores": f"{req.cpu_cores.mid:.2f}",
                    "Memory (GiB)": f"{req.mem_gib.mid:.2f}",
                    "Disk (GiB)": f"{req.disk_gib.mid:.2f}",
                    "Network (Mbps)": f"{req.network_mbps.mid:.2f}"
                })

            # Display cluster information
            st.header("Cluster Information")
            if plan.candidate_clusters.zonal:
                cluster = plan.candidate_clusters.zonal[0]
                st.write({
                    "Nodes per Zone": cluster.count,
                    "Total Regional Nodes": cluster.count * 3,
                    "Annual Cost per Zone": f"${cluster.annual_cost:,.2f}",
                    "Total Annual Cost": f"${plan.candidate_clusters.annual_costs['kafka.zonal-clusters']:,.2f}"
                })
        else:
            st.error("No valid capacity plan could be generated with the given parameters.")

    except Exception as e:
        st.error(f"Error generating capacity plan: {str(e)}")

st.markdown("""
### Notes:
- High Availability (HA) clusters use 2 copies of data
- Strong consistency clusters require at least 3 copies
- Instance requirements: minimum 2 CPU cores and 12 GiB memory
- All calculations assume GP3 EBS volumes
""")
