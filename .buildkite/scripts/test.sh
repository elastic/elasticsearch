#!/bin/bash

set -euo pipefail

bash -c 'sleep 2; echo 2;' &
bash -c 'sleep 3; echo 3;' &
bash -c 'sleep 4; echo 4;' &
bash -c 'sleep 5; echo 5;' &
bash -c 'sleep 6; echo 6;' &
bash -c 'sleep 7; echo 7;' &
bash -c 'sleep 8; echo 8;' &
bash -c 'sleep 9; echo 9;' &
