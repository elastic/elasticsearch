/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcWarningsTestCase;
import org.junit.ClassRule;

public class SingleNodeJdbcWarningsIT extends JdbcWarningsTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = singleNodeCluster();

    @Override
    public ElasticsearchCluster getCluster() {
        return cluster;
    }
}
