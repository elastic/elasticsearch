/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

/**
 * Parses ES|QL datafeed queries. ML cannot depend on the ES|QL plugin (ES|QL extends ML), so the ES|QL plugin implements this
 * interface and registers it in {@code META-INF/services}; {@code MachineLearning.loadExtensions} loads it.
 */
public interface EsqlQueryParser {

    /**
     * Parses {@code query}.
     *
     * @throws org.elasticsearch.ElasticsearchException with status {@code BAD_REQUEST} when the query is not valid ES|QL
     */
    ParsedEsqlQuery parse(String query);
}
