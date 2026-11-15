/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Set;

/**
 * A fixed-span time grouping of a STATS command: {@code BUCKET(field, span)}, {@code DATE_TRUNC(span, field)} or
 * {@code TBUCKET(span)}, used directly as a grouping or through an attribute defined below the STATS by EVAL, RENAME
 * or the grouping of a lower STATS.
 *
 * @param field         the bucketed field as written where the time function is called, or {@code null} for {@code TBUCKET},
 *                      which buckets the implicit time-series timestamp; not resolved against mappings
 * @param span          the fixed bucket span
 * @param outputColumns the columns of the final query output that carry the bucket value: the grouping column, followed
 *                      through RENAME, EVAL aliases that derive only from it and literals, KEEP, DROP and later STATS;
 *                      empty when the bucket value does not reach the output
 */
public record TimeBucket(@Nullable String field, TimeValue span, Set<String> outputColumns) {

    public TimeBucket {
        Objects.requireNonNull(span);
        outputColumns = Set.copyOf(outputColumns);
    }
}
