/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.core.Nullable;

import java.util.Objects;
import java.util.Set;

/**
 * One processing command of the top-level ES|QL pipeline.
 *
 * @param kind             the command kind
 * @param referencedFields field names the command reads, unquoted
 * @param bucket           for {@link Kind#STATS}, the fixed-span time bucket among its groupings if there is exactly one
 */
public record PipelineStep(Kind kind, Set<String> referencedFields, @Nullable TimeBucket bucket) {

    public enum Kind {
        LIMIT,
        STATS,
        WHERE,
        SORT,
        /** A command that merges several pipelines ({@code FORK}, {@code FROM} with subqueries). Commands below it are not listed. */
        MERGE,
        OTHER
    }

    public PipelineStep {
        Objects.requireNonNull(kind);
        referencedFields = Set.copyOf(referencedFields);
    }

    public PipelineStep(Kind kind, Set<String> referencedFields) {
        this(kind, referencedFields, null);
    }
}
