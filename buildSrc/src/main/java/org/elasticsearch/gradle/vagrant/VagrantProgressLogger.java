/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.vagrant;

import java.util.function.UnaryOperator;

public class VagrantProgressLogger implements UnaryOperator<String> {

    private static final String HEADING_PREFIX = "==> ";

    private final String squashedPrefix;
    private String lastLine = "";
    private String heading = "";
    private boolean inProgressReport = false;

    public VagrantProgressLogger(String squashedPrefix) {
        this.squashedPrefix = squashedPrefix;
    }

    @Override
    public String apply(String line) {
        if (line.startsWith("\r\u001b")) {
            /* We don't want to try to be a full terminal emulator but we want to
              keep the escape sequences from leaking and catch _some_ of the
              meaning. */
            line = line.substring(2);
            if ("[K".equals(line)) {
                inProgressReport = true;
            }
            return null;
        }
        if (line.startsWith(squashedPrefix)) {
            line = line.substring(squashedPrefix.length());
            inProgressReport = false;
            lastLine = line;
            if (line.startsWith(HEADING_PREFIX)) {
                line = line.substring(HEADING_PREFIX.length());
                heading = line + " > ";
            } else {
                line = heading + line;
            }
        } else if (inProgressReport) {
            inProgressReport = false;
            line = lastLine + line;
        } else {
            return null;
        }
        return line;
    }
}
