/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.ConnectionTestCase;
import org.junit.ClassRule;

public class MultiNodeJdbcConnectionIT extends ConnectionTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = multiNodeCluster();

    @Override
    public ElasticsearchCluster getCluster() {
        return cluster;
    }
}
