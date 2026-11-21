/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.core.Nullable;

/**
 * Reads the structure of an ES|QL query for ES|QL datafeeds. ML cannot depend on the ES|QL plugin (ES|QL extends ML), so ES|QL
 * implements this interface and registers it in {@code META-INF/services}; {@code MachineLearning.loadExtensions} loads it.
 * Implementations report what the query says and leave every datafeed decision to ML.
 */
public interface EsqlQueryAnalyzer {

    /**
     * Parses {@code query}.
     *
     * @throws org.elasticsearch.ElasticsearchException with status {@code BAD_REQUEST} when the query is not valid ES|QL
     */
    EsqlQueryStructure analyze(String query);

    /**
     * The source field whose value {@code outputColumn} carries in the output of the analyzed query, traced through RENAME, EVAL,
     * KEEP and DROP; {@code null} when the column is computed or does not reach the output. For queries without STATS.
     */
    @Nullable
    String sourceFieldOf(EsqlQueryStructure structure, String outputColumn);

    /** Whether an ES|QL response column type (for example {@code date} or {@code date_nanos}) holds dates. */
    boolean isDateType(String outputType);
}
