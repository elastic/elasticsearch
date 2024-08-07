#!/bin/bash

set -euo pipefail

VALIDATION_SCRIPTS_VERSION=2.5.1
GRADLE_ENTERPRISE_ACCESS_KEY=$(vault kv get -field=value secret/ci/elastic-elasticsearch/gradle-enterprise-api-key)
export GRADLE_ENTERPRISE_ACCESS_KEY

curl -s -L -O https://github.com/gradle/gradle-enterprise-build-validation-scripts/releases/download/v$VALIDATION_SCRIPTS_VERSION/gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip && unzip -q -o gradle-enterprise-gradle-build-validation-$VALIDATION_SCRIPTS_VERSION.zip

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

set +e
gradle-enterprise-gradle-build-validation/03-validate-local-build-caching-different-locations.sh -r https://github.com/elastic/elasticsearch.git -b $BUILDKITE_BRANCH --gradle-enterprise-server https://gradle-enterprise.elastic.co -t precommit --fail-if-not-fully-cacheable | tee $tmpOutputFile
# Capture the return value
retval=$?
set -e

# Now read the content from the temporary file into a variable
perfOutput=$(cat $tmpOutputFile | sed -n '/Performance Characteristics/,/See https:\/\/gradle.com\/bvs\/main\/Gradle.md#performance-characteristics for details./p' | sed '$d' | sed 's/\x1b\[[0-9;]*m//g')
investigationOutput=$(cat $tmpOutputFile | sed -n '/Investigation Quick Links/,$p' | sed 's/\x1b\[[0-9;]*m//g')

# Initialize HTML output variable
summaryHtml="<h4>Build Cache Performance Characteristics</h4>"
summaryHtml+="<ul>"

# Process each line of the string
while IFS=: read -r label value; do
    if [[ -n "$label" && -n "$value" ]]; then
        # Trim whitespace from label and value
        trimmed_label=$(echo "$label" | xargs)
        trimmed_value=$(echo "$value" | xargs)

        # Append to HTML output variable
        summaryHtml+="<li><strong>$trimmed_label:</strong> $trimmed_value</li>"
    fi
done <<< "$perfOutput"

summaryHtml+="</ul>"

# generate html for links
summaryHtml+="<h4>Investigation Links</h4>"
summaryHtml+="<ul>"

# Process each line of the string
while IFS= read -r line; do
    if [[ "$line" =~ http.* ]]; then
        # Extract URL and description using awk
        url=$(echo "$line" | awk '{print $NF}')
        description=$(echo "$line" | sed -e "s/:.*//")

        # Append to HTML output variable
        summaryHtml+="    <li><a href=\"$url\">$description</a></li>"
    fi
done <<< "$investigationOutput"

# End of the HTML content
summaryHtml+="</ul>"

cat << EOF | buildkite-agent annotate --context "ctx-validation-summary" --style "info"
$summaryHtml
EOF

# Check if the command was successful
if [ $retval -eq 0 ]; then
    echo "Experiment completed successfully"
elif [ $retval -eq 1 ]; then
    echo "An invalid input was provided while attempting to run the experiment"
elif [ $retval -eq 2 ]; then
    echo "One of the builds that is part of the experiment failed"
elif [ $retval -eq 3 ]; then
    echo "The build was not fully cacheable for the given task graph"
elif [ $retval -eq 3 ]; then
    echo "An unclassified, fatal error happened while running the experiment"
fi

exit $retval

