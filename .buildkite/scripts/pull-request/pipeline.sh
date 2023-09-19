#!/bin/bash

set -euo pipefail

npm install -g bun
bun .buildkite/scripts/pull-request/pipeline.generate.ts
