/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

/**
 * Canonical field name resolved from raw JSON bytes, optionally carrying a dense
 * ordinal assigned when the {@link FieldNameLookup} table is frozen.
 *
 * @param name    interned field name
 * @param ordinal dense index ({@code 0..N-1}) after freeze, or {@code -1} while learning
 */
public record ResolvedFieldName(String name, int ordinal) {

    /** Sentinel for a scan that encountered a backslash escape in the field name. */
    public static final ResolvedFieldName ESCAPED = new ResolvedFieldName(null, -1);

    public boolean isEscaped() {
        return this == ESCAPED;
    }
}
