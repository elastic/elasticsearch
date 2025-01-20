/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.cli.LenientTestCase;
import org.junit.ClassRule;

public class CliLenientIT extends LenientTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = SqlTestCluster.getCluster(false);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
