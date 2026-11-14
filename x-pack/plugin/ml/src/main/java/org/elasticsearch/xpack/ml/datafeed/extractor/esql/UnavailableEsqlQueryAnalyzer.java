/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure;

/** Used when no ES|QL plugin provides an {@link EsqlQueryAnalyzer}: every ES|QL datafeed request fails with 400. */
public final class UnavailableEsqlQueryAnalyzer implements EsqlQueryAnalyzer {

    @Override
    public EsqlQueryStructure analyze(String query) {
        throw unavailable();
    }

    @Override
    public boolean isDateType(String outputType) {
        throw unavailable();
    }

    private static ElasticsearchStatusException unavailable() {
        return new ElasticsearchStatusException("ES|QL datafeeds require the ES|QL plugin, which is not available", RestStatus.BAD_REQUEST);
    }
}
