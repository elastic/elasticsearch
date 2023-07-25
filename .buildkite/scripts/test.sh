#!/bin/bash

set -euo pipefail

bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 600; echo 2;' &

exit 1
