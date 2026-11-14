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

/**
 * A fixed-span time grouping: {@code BUCKET(field, span)}, {@code DATE_TRUNC(span, field)} or {@code TBUCKET(span)}.
 *
 * @param field the bucketed field, or {@code null} for {@code TBUCKET}, which buckets the implicit time-series timestamp
 */
public record TimeBucket(@Nullable String field, TimeValue span) {

    public TimeBucket {
        Objects.requireNonNull(span);
    }
}
