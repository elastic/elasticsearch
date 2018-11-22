#!/bin/bash

set -e

addprinc.sh elasticsearch
addprinc.sh HTTP/localhost
addprinc.sh peppa
addprinc.sh george         dino

sleep infinity