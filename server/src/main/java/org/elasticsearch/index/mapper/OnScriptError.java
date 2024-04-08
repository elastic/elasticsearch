/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.util.Locale;

/**
 * Represents the behaviour when a runtime field or an index-time script fails: either fail and raise the error,
 * or continue and ignore the error.
 */
public enum OnScriptError {
    FAIL,
    CONTINUE;

    @Override
    public final String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
