/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.DataLoader;
import org.elasticsearch.xpack.sql.qa.jdbc.SqlSpecTestCase;
import org.junit.ClassRule;

public class JdbcSqlSpecIT extends SqlSpecTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = SqlTestCluster.getCluster();

    @Override
    protected void loadDataset(RestClient client) throws Exception {
        DataLoader.loadDatasetIntoEs(client);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public JdbcSqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, String query) {
        super(fileName, groupName, testName, lineNumber, query);
    }
}
