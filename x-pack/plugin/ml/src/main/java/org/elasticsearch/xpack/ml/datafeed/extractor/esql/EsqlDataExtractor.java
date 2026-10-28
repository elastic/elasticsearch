/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

/**
 * ES|QL datafeed extraction. The {@link org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor} implementation is stacked in a
 * companion change; this class holds row-limit constants shared with
 * {@link org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractor}.
 */
public class EsqlDataExtractor {

    /**
     * Row limit injected into a user's ES|QL query when it has no explicit outer {@code LIMIT}. Callers that detect
     * genuine row-count truncation (e.g. {@code ChunkedDataExtractor}) must compare against this constant, not
     * {@code esql.query.result_truncation_default_size}, so chunk sizing and truncation detection cannot drift apart.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;

    /**
     * Whether the user's pipeline contains a depth-zero {@code STATS} command (aggregating query).
     */
    public static boolean hasAggregation(String esqlQuery) {
        return EsqlQueryClauseScanner.hasAggregation(esqlQuery);
    }

    private EsqlDataExtractor() {}
}
