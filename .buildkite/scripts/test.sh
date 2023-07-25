#!/bin/bash

set -euo pipefail

(winpty bash -c 'trap "" SIGTERM SIGINT 0 1 2 3 13 15; sleep 600; echo 2;') &

exit 1
