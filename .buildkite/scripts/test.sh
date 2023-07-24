#!/bin/bash

set -euo pipefail

bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 2; echo 2; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 3; echo 3; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 4; echo 4; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 5; echo 5; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 6; echo 6; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 7; echo 7; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 8; echo 8; cat; exit 0' &
bash -c 'trap "echo trap" 0 1 2 3 13 15; sleep 9; echo 9; cat; exit 0' &

exit 1
