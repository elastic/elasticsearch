/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.jdbc.DatabaseMetaDataTestCase;
import org.junit.ClassRule;

public class JdbcDatabaseMetaDataIT extends DatabaseMetaDataTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = SqlTestCluster.getCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String clusterName() {
        // `GET /` on Serverless returns a response with `cluster_name: <project_id>`, so we need to use a fixed string.
        return SqlTestCluster.CLUSTER_NAME;
    }
}
