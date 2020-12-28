"""The main module."""
import json
from typing import Optional

import typer
from jsonbender import BendingException, F, S, bend
from jsonbender.list_ops import ForallBend
from loguru import logger
from requests import Session
from requests.auth import HTTPBasicAuth


def count_errors(item):
    """Count process group errors."""
    if item:
        count = 0
        for x in item:
            if x["level"] == "ERROR":
                count = count + 1
        return count
    return 0


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


def get_api_response(session: Session, process_group_id: Optional[str] = None):
    """Get API response for process group."""
    if process_group_id is None:
        process_group_id = "root"
    response = session.get(
        f"https://nifi-production.ona.io/nifi-api/flow/process-groups/{process_group_id}"
    )
    return response.json()


def process_api_response(
    response: dict, session: Session, current_depth: int = 0, max_depth: int = 2
):
    """Process JSON received from API."""
    try:
        result = bend(MAPPING, response)
    except BendingException:
        logger.exception("What?!")
    else:
        if result["processGroups"]:  # check if processGroups exist
            for process_group in result["processGroups"]:
                # log details for each process group
                logger.info(json.dumps(process_group))
                # try get the details of this process group's children
                if current_depth < max_depth:
                    json_content = get_api_response(
                        session=session, process_group_id=process_group["id"],
                    )
                    process_api_response(
                        response=json_content,
                        session=session,
                        current_depth=current_depth + 1,
                        max_depth=max_depth,
                    )


app = typer.Typer()


@app.command()
def nifi(max_depth: int = 0, log_file: str = "/tmp/nifi-monitor.log"):
    """Monitor NiFi process groups."""
    logger.add(
        sink=log_file, format="{time} {level}", serialize=True, enqueue=True,
    )
    session = Session()
    session.auth = HTTPBasicAuth("admin", "Defend-Paint-Record-Express-0")
    # get the JSON response for root-level process groups
    json_content = get_api_response(session=session)
    # recursively process the API response
    return process_api_response(
        response=json_content, session=session, current_depth=0, max_depth=max_depth,
    )


if __name__ == "__main__":
    app()
