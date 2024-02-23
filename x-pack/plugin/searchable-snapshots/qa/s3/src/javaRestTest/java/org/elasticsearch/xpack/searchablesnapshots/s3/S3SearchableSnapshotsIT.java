/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.s3;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class S3SearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {
    static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .keystore("s3.client.searchable_snapshots.access_key", System.getProperty("s3AccessKey"))
        .keystore("s3.client.searchable_snapshots.secret_key", System.getProperty("s3SecretKey"))
        .setting("xpack.license.self_generated.type", "trial")
        .setting("s3.client.searchable_snapshots.protocol", () -> "http", (n) -> USE_FIXTURE)
        .setting("s3.client.searchable_snapshots.endpoint", s3Fixture::getAddress, (n) -> USE_FIXTURE)
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String writeRepositoryType() {
        return "s3";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
