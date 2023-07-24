#!/bin/bash

set -euo pipefail

bash -c 'sleep 2; echo 2; exit 0' &
bash -c 'sleep 3; echo 3; exit 0' &
bash -c 'sleep 4; echo 4; exit 0' &
bash -c 'sleep 5; echo 5; exit 0' &
bash -c 'sleep 6; echo 6; exit 0' &
bash -c 'sleep 7; echo 7; exit 0' &
bash -c 'sleep 8; echo 8; exit 0' &
bash -c 'sleep 9; echo 9; exit 0' &

exit 1
