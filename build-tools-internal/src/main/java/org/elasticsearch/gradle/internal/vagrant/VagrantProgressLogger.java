/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.vagrant;

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
