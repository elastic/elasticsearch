/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.hdfs.HdfsClientThreadLeakFilter;
import org.elasticsearch.test.fixtures.hdfs.HdfsFixture;
import org.elasticsearch.test.fixtures.krb5kdc.Krb5kDcContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakFilters(filters = { HdfsClientThreadLeakFilter.class, TestContainersThreadFilter.class })
public class SecureHdfsSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    public static Krb5kDcContainer krb5Fixture = new Krb5kDcContainer();

    public static HdfsFixture hdfsFixture = new HdfsFixture().withKerberos(() -> krb5Fixture.getPrincipal(), () -> krb5Fixture.getKeytab());

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("repository-hdfs")
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .systemProperty("java.security.krb5.conf", () -> krb5Fixture.getConfPath().toString())
        .configFile("repository-hdfs/krb5.keytab", Resource.fromFile(() -> krb5Fixture.getEsKeytab()))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(krb5Fixture).around(hdfsFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String writeRepositoryType() {
        return "hdfs";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String uri = "hdfs://localhost:" + hdfsFixture.getPort();
        final String path = "/user/elasticsearch/test/searchable_snapshots/secure";
        Settings.Builder repositorySettings = Settings.builder().put("client", "searchable_snapshots").put("uri", uri).put("path", path);

        final String principal = "elasticsearch@BUILD.ELASTIC.CO";
        repositorySettings.put("security.principal", principal);
        return repositorySettings.build();
    }

}
