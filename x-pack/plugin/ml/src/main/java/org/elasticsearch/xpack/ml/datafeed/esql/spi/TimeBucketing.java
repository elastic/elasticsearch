/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Set;

/**
 * How the top-most {@code STATS} of a query buckets time, so that a datafeed can derive its {@code grouping_interval}.
 * A bucket is {@code BUCKET}, {@code BIN}, {@code DATE_TRUNC} or {@code TBUCKET}, used as a grouping or through an attribute
 * that EVAL, RENAME or a lower STATS defines. Names are as written and not resolved against mappings.
 */
public sealed interface TimeBucketing {

    /** The query does not group by a time bucket. */
    record NoTimeBucket() implements TimeBucketing {}

    /**
     * The query groups by a bucket of constant {@code span}.
     *
     * @param outputColumns the columns of the query output that carry the bucket value, followed from the grouping through RENAME,
     *                      EVAL aliases that derive only from it, KEEP, DROP and later STATS; empty if it does not reach the output
     */
    record FixedTimeBucket(TimeValue span, BucketSource source, Set<String> outputColumns) implements TimeBucketing {
        public FixedTimeBucket {
            Objects.requireNonNull(span);
            Objects.requireNonNull(source);
            outputColumns = Set.copyOf(outputColumns);
        }
    }

    /** The query groups by a time bucket that cannot give a fixed interval. */
    record UnsupportedTimeBucket(Reason reason) implements TimeBucketing {
        public UnsupportedTimeBucket {
            Objects.requireNonNull(reason);
        }
    }

    /** What a fixed bucket reads. */
    sealed interface BucketSource {
        /** A source field, traced from the bucket's field argument through EVAL and RENAME. */
        record SourceField(String name) implements BucketSource {}

        /** The implicit {@code @timestamp} of a time-series source, as in {@code TBUCKET}. */
        record ImplicitTimestamp() implements BucketSource {}

        /** A value computed in the query, for example {@code TO_DATETIME(raw)}. */
        record ComputedValue() implements BucketSource {}
    }

    enum Reason {
        /** The STATS groups by more than one time bucket. */
        SEVERAL_BUCKETS,
        /** The span is a calendar period such as {@code 1 month}, which has no fixed length. */
        CALENDAR_SPAN,
        /** The bucket count and range choose the span, as in {@code BUCKET(ts, 20, from, to)}. */
        AUTO_SPAN
    }
}
