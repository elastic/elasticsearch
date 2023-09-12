#!/bin/bash

set -euo pipefail

npm install -g bun
bun .buildkite/pipelines/pull-request.ts | buildkite-agent pipeline upload
