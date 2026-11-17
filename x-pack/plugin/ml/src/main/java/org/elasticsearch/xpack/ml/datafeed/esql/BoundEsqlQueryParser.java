/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

/**
 * An {@link EsqlQueryParser} tied to the node's {@link ClusterService}, so datafeed code parses with the settings the cluster runs
 * with without passing the service to every caller. The provider itself is loaded before any cluster service exists.
 */
@FunctionalInterface
public interface BoundEsqlQueryParser {

    ParsedEsqlQuery parse(String query);

    static BoundEsqlQueryParser of(EsqlQueryParser parser, ClusterService clusterService) {
        return query -> parser.parse(query, clusterService);
    }

    /** Rejects every ES|QL datafeed with a 400; for callers that never parse and for nodes without the ES|QL plugin. */
    static BoundEsqlQueryParser unavailable() {
        UnavailableEsqlQueryParser unavailable = new UnavailableEsqlQueryParser();
        return query -> unavailable.parse(query, null);
    }
}
