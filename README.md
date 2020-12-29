# Canopy Connector Monitoring Tool

This is intended as a simple to monitor Canopy connectors.

At the moment only NiFi-based connectors are supported.

## How it works

### Basic Connector Health Monitoring (NiFi-based connectors)

This tool works on the premise that one can infer connector health based on:

- The number of stopped processors in the connector process group
- The number of errors encountered by the connector process group
- The number of queued flow-files in the connector process group

Therefore, making it possible to monitor the above means that we are able to detect potential problems with connectors.

This is done by a command line application that:

1. Reads the NiFi API to get information about process groups.  The response from the API is slightly manipulated to
   expose the required information.
2. Writes the formatted process groups to a log file.  Each line of the log file contains a JSON document that represents one NiFi process group's information at a particular moment in time. [Here's a sample log file](docs/sample.log).

## Installation

### Requirements

- Python ^3.8
- [Poetry](https://python-poetry.org/)

### Quick start

- Ensure that you have the requirements listed above installed
- Clone this repository
- Use poetry to install all the python dependencies, like so:

    ```sh
    poetry env use 3.8  # ensures poetry uses Python 3.8
    poetry install --no-dev  # installs python packages for production (no dev)
    ```

- If the target NiFi application is protected by basic auth, then set an environment variable with the password.  Leave this out completely if the NiFi installation is not password-protected.

    ```sh
    set -gx NIFI_USER_PASSWORD hunter2  # fish shell
    ```

    ```bash
    export NIFI_USER_PASSWORD="hunter2"  # bash
    ```

- Run this command periodically to get and process NiFi process group information.  The intention is that this command should be run periodically so that the health of the connectors can be monitored over time.

    ```sh
    poetry run python main.py --nifi-base-url="https://nifi-production.ona.io" --log-file="/tmp/nifi.log" --max-depth=1 --nifi-username="NiFi username"
    ```

- Run this for more options:

    ```sh
    poetry run python main.py --help
    ```

## Contributing

- Ensure that you have the requirements listed above installed
- Clone this repository
- Use poetry to set up the development environment, like so:

    ```sh
    poetry env use 3.8
    poetry install
    poetry shell
    pre-commit install
    ```

- ???
- Profit

## TODO

- Add tests
