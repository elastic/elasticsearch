/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Heap weights for datasource values charged to a circuit breaker. Lives in {@code spi} so format readers can charge
 * the same estimate without depending on the resolver.
 */
public final class HeapEstimates {

    /**
     * Allowance for a {@code ReferenceAttribute} plus its {@code NameId}, excluding the column name. Not a measured
     * deep size.
     */
    private static final long COLUMN_SHELL_BYTES = 128L;

    private HeapEstimates() {}

    /**
     * Heap one schema column keeps reachable: the attribute shell plus its name. The name is charged because a
     * flattened nested field is named by its whole dotted path, so a column's name can dwarf the rest of it and a
     * schema's size is then the sum of its names, not its column count.
     *
     * @param nameLength {@code String#length()} of the column name
     */
    public static long columnBytes(int nameLength) {
        return COLUMN_SHELL_BYTES + stringBytes(nameLength);
    }

    /**
     * About 40 bytes for the {@code String} object and its backing array headers on a 64-bit JVM with compressed
     * references, plus two bytes per character. Both parts round up on purpose (compact Latin-1 strings use one byte
     * per character); this feeds a circuit breaker, where over-counting trips a little early and under-counting lets
     * the work outgrow its budget.
     */
    public static long stringBytes(@Nullable String s) {
        return stringBytes(s != null ? s.length() : 0);
    }

    /**
     * {@link #stringBytes(String)} for a string of {@code length} characters that has not been built yet, so a charge
     * can precede the allocation it covers.
     */
    public static long stringBytes(int length) {
        return 40 + length * (long) Character.BYTES;
    }
}
