/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class })
public class HdfsRepositoryAnalysisRestIT extends AbstractHdfsRepositoryAnalysisRestIT {

    public static HdfsFixture hdfsFixture = new HdfsFixture();

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
    protected String getRepositoryPath() {
        return "/user/elasticsearch/test/repository_test_kit/simple";
    }

    @Override
    protected int getHdfsPort() {
        return hdfsFixture.getPort();
    }
}
