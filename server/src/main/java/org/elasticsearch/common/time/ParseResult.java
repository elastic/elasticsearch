/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.core.Nullable;

import java.time.temporal.TemporalAccessor;

/**
 * The result of the parse. If successful, {@code result} will be non-null.
 * If parse failed, {@code errorIndex} specifies the index into the parsed string
 * that the first invalid data was encountered.
 */
record ParseResult(@Nullable TemporalAccessor result, int errorIndex) {
    ParseResult(TemporalAccessor result) {
        this(result, -1);
    }

    static ParseResult error(int errorIndex) {
        return new ParseResult(null, errorIndex);
    }
}
