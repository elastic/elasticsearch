#!/bin/bash
echo "Network operation failed during tests: Could not GET from host artifactory.elstc.co (Received status code 403 Forbidden). The environment needs to be fixed to download Shibboleth dependencies." >&2
exit 1
