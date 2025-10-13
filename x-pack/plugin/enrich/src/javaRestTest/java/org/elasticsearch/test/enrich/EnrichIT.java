/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.enrich;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

public class EnrichIT extends CommonEnrichRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = enrichCluster("basic", false).build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
