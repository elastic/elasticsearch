/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class GCSSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    private static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-gcs")
        .module("searchable-snapshots")
        .module("lang-painless")
        .setting("gcs.client.searchable_snapshots.endpoint", () -> fixture.getAddress(), s -> USE_FIXTURE)
        .setting("gcs.client.searchable_snapshots.token_uri", () -> fixture.getAddress() + "/o/oauth2/token", s -> USE_FIXTURE)
        .apply(c -> {
            if (USE_FIXTURE) {
                c.keystore(
                    "gcs.client.searchable_snapshots.credentials_file",
                    Resource.fromString(() -> new String(TestUtils.createServiceAccount(random()), StandardCharsets.UTF_8))
                );
            } else {
                c.keystore(
                    "gcs.client.searchable_snapshots.credentials_file",
                    Resource.fromFile(PathUtils.get(System.getProperty("test.google.account")))
                );
            }
        })
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String writeRepositoryType() {
        return "gcs";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String bucket = System.getProperty("test.gcs.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.gcs.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("bucket", bucket).put("base_path", basePath).build();
    }
}
