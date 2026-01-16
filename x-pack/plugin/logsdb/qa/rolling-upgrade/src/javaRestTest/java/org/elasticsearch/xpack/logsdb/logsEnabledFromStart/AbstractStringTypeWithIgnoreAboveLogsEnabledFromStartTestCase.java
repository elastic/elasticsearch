/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.logsEnabledFromStart;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.logsdb.AbstractStringTypeWithIgnoreAboveLogsdbRollingUpgradeTestCase;
import org.elasticsearch.xpack.logsdb.Clusters;
import org.junit.ClassRule;

public abstract class AbstractStringTypeWithIgnoreAboveLogsEnabledFromStartTestCase extends
    AbstractStringTypeWithIgnoreAboveLogsdbRollingUpgradeTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionClusterWithLogsEnabled(USER, PASS);

    protected AbstractStringTypeWithIgnoreAboveLogsEnabledFromStartTestCase(
        String dataStreamName,
        String template,
        Mapper.IgnoreAbove ignoreAbove
    ) {
        super(dataStreamName, template, ignoreAbove);
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
