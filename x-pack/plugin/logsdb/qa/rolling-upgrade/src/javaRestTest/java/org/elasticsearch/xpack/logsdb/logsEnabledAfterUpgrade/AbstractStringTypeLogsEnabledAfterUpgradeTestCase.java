/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledAfterUpgrade;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.logsdb.AbstractStringTypeLogsdbRollingUpgradeTestCase;
import org.elasticsearch.xpack.logsdb.Clusters;
import org.junit.ClassRule;

/**
 * In this scenario, the cluster starts in standard mode, upgrades fully, and then LogsDB mode is enabled with a rollover.
 */
public abstract class AbstractStringTypeLogsEnabledAfterUpgradeTestCase extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionClusterWithLogsDisabled(USER, PASS);

    protected AbstractStringTypeLogsEnabledAfterUpgradeTestCase(String dataStreamName, String template) {
        super(dataStreamName, template);
    }

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    public void testIndexingWithLogsEnabledAfterUpgrade() throws Exception {
        createIndex();
        indexDocumentsAndVerifyResults();
        verifyIndexMode(IndexMode.STANDARD);

        for (int i = 0; i < getNumNodes(); i++) {
            upgradeNode(i);
            indexDocumentsAndVerifyResults();
        }

        enableLogsDb();
        rolloverDataStream();
        verifyIndexMode(IndexMode.LOGSDB);

        indexDocumentsAndVerifyResults();
    }
}
