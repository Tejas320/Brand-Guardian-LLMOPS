import os
import logging
from azure.monitor.opentelemetry import configure_azure_monitor

logger = logging.getLogger("brand-guardian-telemetry")

def setup_telemetry():
    connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not connection_string:
        logger.warning("No instrumentation key found. Telemetry is DISABLED.")
        return
    try:
        configure_azure_monitor(
            connection_string=connection_string
        )
        logger.info("Azure Monitor Tracking Enabled and Connected")
    except Exception as e:
        logger.error(f"Failed to initialize Azure Monitor : {e}")