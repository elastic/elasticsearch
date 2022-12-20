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

public enum ErrorBehaviour {
    FAIL,
    CONTINUE;

    public static ErrorBehaviour fromString(final String str) {
        Objects.requireNonNull(str, "input string is null");
        return switch (str.toLowerCase(Locale.ROOT)) {
            case "fail" -> FAIL;
            case "continue" -> CONTINUE;
            default -> throw new IllegalArgumentException("Unknown ErrorBehavior [" + str + "]");
        };
    }
}
