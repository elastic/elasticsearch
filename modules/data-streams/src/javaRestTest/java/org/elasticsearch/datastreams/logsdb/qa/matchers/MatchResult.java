/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers;

import java.util.Objects;

public class MatchResult {
    private final boolean isMatch;
    private final String message;

    private MatchResult(boolean isMatch, String message) {
        this.isMatch = isMatch;
        this.message = message;
    }

    public static MatchResult match() {
        return new MatchResult(true, "Match successful");
    }

    public static MatchResult noMatch(final String reason) {
        return new MatchResult(false, reason);
    }

    public boolean isMatch() {
        return isMatch;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchResult that = (MatchResult) o;
        return isMatch == that.isMatch && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isMatch, message);
    }

    @Override
    public String toString() {
        return "MatchResult{" + "isMatch=" + isMatch + ", message='" + message + '\'' + '}';
    }
}
