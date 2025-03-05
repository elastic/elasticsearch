/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Integration test that runs against an HA-Enabled HDFS instance
 */
@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class })
public class HaHdfsFailoverTestSuiteIT extends AbstractHaHdfsFailoverTestSuiteIT {

    public static HdfsFixture hdfsFixture = new HdfsFixture().withHAService("ha-hdfs");

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("repository-hdfs")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(hdfsFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    HdfsFixture getHdfsFixture() {
        return hdfsFixture;
    }
}
