/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.elasticsearch.test.fixtures.krb5kdc.Krb5kDcContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class, TestContainersThreadFilter.class })
public class SecureHdfsRepositoryAnalysisRestIT extends AbstractHdfsRepositoryAnalysisRestIT {

    public static Krb5kDcContainer krb5Fixture = new Krb5kDcContainer();

    public static HdfsFixture hdfsFixture = new HdfsFixture().withKerberos(() -> krb5Fixture.getPrincipal(), () -> krb5Fixture.getKeytab());

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("repository-hdfs")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .systemProperty("java.security.krb5.conf", () -> krb5Fixture.getConfPath().toString())
        .configFile("repository-hdfs/krb5.conf", Resource.fromString(() -> krb5Fixture.getConf()))
        .configFile("repository-hdfs/krb5.keytab", Resource.fromFile(() -> krb5Fixture.getEsKeytab()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(krb5Fixture).around(hdfsFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected int getHdfsPort() {
        return hdfsFixture.getPort();
    }

    @Override
    protected String getRepositoryPath() {
        return "/user/elasticsearch/test/repository_test_kit/secure";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put(super.repositorySettings()).put("security.principal", "elasticsearch@BUILD.ELASTIC.CO").build();
    }
}
