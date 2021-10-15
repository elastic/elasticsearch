#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.
#

<% /* Populated by Gradle */ %>
VERSION="$version"

plugin_name_is_next=0

declare -a args_array

while test \$# -gt 0; do
  opt="\$1"
  shift

  if [[ \$plugin_name_is_next -eq 1 ]]; then
    if [[ -f "/opt/plugins/archive/\$opt-\${VERSION}.zip" ]]; then
      opt="file:/opt/plugins/archive/\$opt-\${VERSION}.zip"
    fi
  elif [[ "\$opt" == "install" ]]; then
    plugin_name_is_next=1
  fi

  args_array+=("\$opt")
done

set -- "\$@" "\${args_array[@]}"

exec /usr/share/elasticsearch/bin/elasticsearch-plugin "\$@"
