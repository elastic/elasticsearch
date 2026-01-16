/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledFromStart;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.logsdb.AbstractStringTypeLogsdbRollingUpgradeTestCase;
import org.elasticsearch.xpack.logsdb.Clusters;
import org.junit.ClassRule;

/**
 * In this scenario, LogsDB mode is enabled before creating any indices, and the cluster is then upgraded while indexing continues.
 */
public abstract class AbstractStringTypeLogsEnabledFromStartTestCase extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionClusterWithLogsEnabled(USER, PASS);

    protected AbstractStringTypeLogsEnabledFromStartTestCase(String dataStreamName, String template) {
        super(dataStreamName, template);
    }

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    public void testIndexingWithLogsEnabledFromTheStart() throws Exception {
        createIndex();
        indexDocumentsAndVerifyResults();
        verifyIndexMode(IndexMode.LOGSDB);

        for (int i = 0; i < getNumNodes(); i++) {
            upgradeNode(i);
            indexDocumentsAndVerifyResults();
        }

        indexDocumentsAndVerifyResults();
    }
}
