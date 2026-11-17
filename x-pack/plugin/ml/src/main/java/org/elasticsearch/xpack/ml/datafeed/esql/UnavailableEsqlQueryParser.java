/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

/** Used when no ES|QL plugin provides an {@link EsqlQueryParser}: every ES|QL datafeed request fails with 400. */
public final class UnavailableEsqlQueryParser implements EsqlQueryParser {

    @Override
    public ParsedEsqlQuery parse(String query, ClusterService clusterService) {
        throw new ElasticsearchStatusException(
            "ES|QL datafeeds require the ES|QL plugin. The ES|QL plugin is not available.",
            RestStatus.BAD_REQUEST
        );
    }
}
