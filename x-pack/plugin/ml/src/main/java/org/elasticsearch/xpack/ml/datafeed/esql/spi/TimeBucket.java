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
 * @param field         the source field the bucket reads, traced from the time function's field argument down through EVAL and
 *                      RENAME (so {@code EVAL @timestamp = event.ingested} reports {@code event.ingested}); {@code null} when
 *                      {@code computedField} is set and for {@code TBUCKET}, which buckets the implicit time-series timestamp.
 *                      Not resolved against mappings
 * @param computedField whether the bucket reads a computed value (for example {@code TO_DATETIME(raw)}) rather than a source field
 * @param span          the fixed bucket span
 * @param outputColumns the columns of the final query output that carry the bucket value: the grouping column, followed
 *                      through RENAME, EVAL aliases that derive only from it and literals, KEEP, DROP and later STATS;
 *                      empty when the bucket value does not reach the output
 */
public record TimeBucket(@Nullable String field, boolean computedField, TimeValue span, Set<String> outputColumns) {

    public TimeBucket {
        if (computedField && field != null) {
            throw new IllegalArgumentException("a computed bucket field has no source field name but got [" + field + "]");
        }
        Objects.requireNonNull(span);
        outputColumns = Set.copyOf(outputColumns);
    }
}
