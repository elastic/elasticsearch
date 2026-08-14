/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

/**
 * Runs the full {@link OTLPMetricsIndexingRestIT} suite against a cluster with
 * {@code indices.batch_indexing=true}, exercising the ESCF (columnar) fast path in
 * {@code OTLPMetricsTransportAction}.
 */
public class BatchOTLPMetricsIndexingRestIT extends OTLPMetricsIndexingRestIT {

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster(true);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
