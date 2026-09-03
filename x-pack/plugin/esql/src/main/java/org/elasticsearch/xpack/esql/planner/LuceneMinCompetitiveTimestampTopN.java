/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.topn.SharedGlobalTopK;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.core.Nullable;

/**
 * Path B wiring for {@code SORT @timestamp DESC | LIMIT N} over Lucene: shared side-channel state
 * between {@code TopNOperator} and {@code LuceneSourceOperator} for a single LONG datetime sort key.
 *
 * <p>{@code globalTopK} is non-null when the global-merge optimization is wired: a {@link SharedGlobalTopK}
 * that aggregates local heaps across all drivers and publishes tighter competitive bounds than per-driver
 * publishing alone.
 */
public record LuceneMinCompetitiveTimestampTopN(
    SharedMinCompetitive.Supplier supplier,
    String sortFieldName,
    @Nullable SharedGlobalTopK.Supplier globalTopK
) {
    public LuceneMinCompetitiveTimestampTopN(SharedMinCompetitive.Supplier supplier, String sortFieldName) {
        this(supplier, sortFieldName, null);
    }
}
