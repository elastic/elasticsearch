/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.logsdb;

import java.util.regex.Pattern;

public class PatternMatcher {
    private final Pattern pattern;

    private PatternMatcher(final Pattern pattern) {
        this.pattern = pattern;
    }

    public boolean matches(final String value) {
        return value != null && pattern.matcher(value).matches();
    }

    public static PatternMatcher forLogs() {
        return new PatternMatcher(Pattern.compile("logs-[^-]+-[^-]+"));
    }
}
