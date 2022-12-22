/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Locale;
import java.util.Objects;

/**
 * Represents the behaviour when a runtime field or an index-time script fails: either fail and raise the error,
 * or continue and ignore the error.
 */
public enum OnScriptError {
    FAIL,
    CONTINUE;

    /**
     * Parses the on_script_error parameter from a string into its corresponding enum instance
     */
    public static OnScriptError fromString(final String str) {
        Objects.requireNonNull(str, "input string is null");
        return switch (str.toLowerCase(Locale.ROOT)) {
            case "fail" -> FAIL;
            case "continue" -> CONTINUE;
            default -> throw new IllegalArgumentException("Unknown onScriptError [" + str + "]");
        };
    }
}
