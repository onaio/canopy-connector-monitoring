"""The main module."""
import json
from typing import Optional

import typer
from jsonbender import BendingException, F, S, bend
from jsonbender.list_ops import ForallBend
from loguru import logger
from requests import Session
from requests.auth import HTTPBasicAuth

VALID_STATUS_CODE = 200


def count_errors(item):
    """Count process group errors."""
    count = 0
    if item:
        for x in item:
            if x["level"] == "ERROR":
                count = count + 1
    return count


# define the mapping to transform the API json to what we want
MAPPING = {
    "processGroupFlowId": S("processGroupFlow", "id"),
    "lastRefreshed": S("processGroupFlow", "lastRefreshed"),
    "processGroups": S("processGroupFlow", "flow", "processGroups")
    >> ForallBend(
        {
            "id": S("component", "id"),
            "name": S("component", "name"),
            "parentGroupId": S("component", "parentGroupId"),
            "runningCount": S("runningCount"),
            "stoppedCount": S("stoppedCount"),
            "invalidCount": S("invalidCount"),
            "disabledCount": S("disabledCount"),
            "activeRemotePortCount": S("activeRemotePortCount"),
            "inactiveRemotePortCount": S("inactiveRemotePortCount"),
            "upToDateCount": S("upToDateCount"),
            "locallyModifiedCount": S("locallyModifiedCount"),
            "staleCount": S("staleCount"),
            "locallyModifiedAndStaleCount": S("locallyModifiedAndStaleCount"),
            "syncFailureCount": S("syncFailureCount"),
            "localInputPortCount": S("localInputPortCount"),
            "localOutputPortCount": S("localOutputPortCount"),
            "publicInputPortCount": S("publicInputPortCount"),
            "publicOutputPortCount": S("publicOutputPortCount"),
            "inputPortCount": S("inputPortCount"),
            "outputPortCount": S("outputPortCount"),
            "statsLastRefreshed": S("status", "statsLastRefreshed"),
            "versionedFlowState": S(
                "status", "aggregateSnapshot", "versionedFlowState"
            ).optional(),
            "flowFilesIn": S("status", "aggregateSnapshot", "flowFilesIn"),
            "bytesIn": S("status", "aggregateSnapshot", "bytesIn"),
            "input": S("status", "aggregateSnapshot", "input"),
            "flowFilesQueued": S(
                "status", "aggregateSnapshot", "flowFilesQueued"
            ),  # noqa
            "bytesQueued": S("status", "aggregateSnapshot", "bytesQueued"),
            "queued": S("status", "aggregateSnapshot", "queued"),
            "queuedCount": S("status", "aggregateSnapshot", "queuedCount"),
            "queuedSize": S("status", "aggregateSnapshot", "queuedSize"),
            "bytesRead": S("status", "aggregateSnapshot", "bytesRead"),
            "read": S("status", "aggregateSnapshot", "read"),
            "bytesWritten": S("status", "aggregateSnapshot", "bytesWritten"),
            "written": S("status", "aggregateSnapshot", "written"),
            "flowFilesOut": S("status", "aggregateSnapshot", "flowFilesOut"),
            "bytesOut": S("status", "aggregateSnapshot", "bytesOut"),
            "output": S("status", "aggregateSnapshot", "output"),
            "flowFilesTransferred": S(
                "status", "aggregateSnapshot", "flowFilesTransferred"
            ),
            "bytesTransferred": S(
                "status", "aggregateSnapshot", "bytesTransferred"
            ),  # noqa
            "transferred": S("status", "aggregateSnapshot", "transferred"),
            "bytesReceived": S("status", "aggregateSnapshot", "bytesReceived"),
            "flowFilesReceived": S(
                "status", "aggregateSnapshot", "flowFilesReceived"
            ),  # noqa
            "received": S("status", "aggregateSnapshot", "received"),
            "bytesSent": S("status", "aggregateSnapshot", "bytesSent"),
            "flowFilesSent": S("status", "aggregateSnapshot", "flowFilesSent"),
            "sent": S("status", "aggregateSnapshot", "sent"),
            "activeThreadCount": S(
                "status", "aggregateSnapshot", "activeThreadCount"
            ),  # noqa
            "terminatedThreadCount": S(
                "status", "aggregateSnapshot", "terminatedThreadCount"
            ),
            "bulletinCount": S("bulletins") >> F(len),
            "errorCount": S("bulletins")
            >> ForallBend(S("bulletin").optional())
            >> F(count_errors),
            "bulletins": S("bulletins")
            >> ForallBend(
                {
                    "id": S("id"),
                    "category": S("bulletin", "category"),
                    "groupId": S("bulletin", "groupId"),
                    "sourceId": S("bulletin", "sourceId"),
                    "sourceName": S("bulletin", "sourceName"),
                    "level": S("bulletin", "level"),
                    "message": S("bulletin", "message"),
                    "timestamp": S("bulletin", "timestamp"),
                }
            ),
        }
    ),
}


class NiFiApiError(Exception):
    """Custom exception to represent API errors."""

    pass


def get_api_response(
    base_url: str, session: Session, process_group_id: Optional[str] = None
):
    """
    Get NiFi API response for process group.

    This function calls the process group API endpoint and retrieves info.
    about a process group's children.
    """
    if process_group_id is None:
        process_group_id = "root"
    response = session.get(
        f"{base_url}/nifi-api/flow/process-groups/{process_group_id}"
    )
    if response.status_code != VALID_STATUS_CODE:
        raise NiFiApiError(
            f"Response Status: {response.status_code}\nText: {response.text}"
        )
    return response.json()


def process_nifi_connectors(
    base_url: str,
    session: Session,
    process_group_id: Optional[str] = None,
    current_depth: int = 0,
    max_depth: int = 2,
):
    """
    Get and process NiFi connectors using the NiFi API.

    This function calls 'get_api_response' to get NiFi processor group info.
    and then logs that information.  This is done recursively until max_depth
    is attained, or all process groups are processed.
    """
    try:
        content = get_api_response(
            base_url=base_url,
            session=session,
            process_group_id=process_group_id,
        )
    except NiFiApiError:
        logger.exception("NiFi API Error")
    else:
        try:
            result = bend(MAPPING, content)
        except BendingException:
            logger.exception("JSON Bending Error")
        else:
            for process_group in result["processGroups"]:
                # log details for each process group
                logger.info(json.dumps(process_group))
                # try get the details of this process group's children
                if current_depth < max_depth:
                    process_nifi_connectors(
                        base_url=base_url,
                        session=session,
                        process_group_id=process_group["id"],
                        current_depth=current_depth + 1,
                        max_depth=max_depth,
                    )


app = typer.Typer()


@app.command()
def nifi(
    nifi_base_url: str = typer.Option(
        "https://nifi-production.ona.io", help="The URL of the NiFi instance."
    ),
    nifi_username: Optional[str] = typer.Option(
        None, help="The NiFi basic auth username.  Leave blank if not required."
    ),
    nifi_password: Optional[str] = typer.Argument(
        None,
        envvar="NIFI_USER_PASSWORD",
        help="The NiFi basic auth password.  Leave blank if not required.",
    ),
    max_depth: int = typer.Option(
        0,
        help="This tool will recursively go through all nested NiFi process "
        "groups until there are no more process groups.  This options is used "
        "to stop the recursion at a certain desired depth.  Use an arbitrarily"
        " large value if you wish to go through all process groups.",
    ),
    log_file: str = typer.Option(
        "/tmp/nifi-monitor.log",
        help="The log file to use.",
    ),
):
    """Monitor NiFi process groups."""
    logger.add(
        sink=log_file,
        format="{time} {level}",
        serialize=True,
        enqueue=True,
        diagnose=False,
    )
    session = Session()
    if nifi_username and nifi_password:
        session.auth = HTTPBasicAuth(nifi_username, nifi_password)
    # recursively get and process the NiFi connectors
    return process_nifi_connectors(
        base_url=nifi_base_url,
        session=session,
        current_depth=0,
        max_depth=max_depth,
    )


if __name__ == "__main__":
    app()
