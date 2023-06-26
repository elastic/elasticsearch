/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit;

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

public class GCSSnapshotRepoTestKitIT extends AbstractSnapshotRepoTestKitRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    private static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-gcs")
        .module("snapshot-repo-test-kit")
        .setting("gcs.client.repository_test_kit.endpoint", () -> fixture.getAddress(), s -> USE_FIXTURE)
        .setting("gcs.client.repository_test_kit.token_uri", () -> fixture.getAddress() + "/o/oauth2/token", s -> USE_FIXTURE)
        .apply(c -> {
            if (USE_FIXTURE) {
                c.keystore(
                    "gcs.client.repository_test_kit.credentials_file",
                    Resource.fromString(() -> new String(TestUtils.createServiceAccount(random()), StandardCharsets.UTF_8))
                );
            } else {
                c.keystore(
                    "gcs.client.repository_test_kit.credentials_file",
                    Resource.fromFile(PathUtils.get(System.getProperty("test.google.account")))
                );
            }
        })
        .apply(c -> {
            if (USE_FIXTURE) {
                // test fixture does not support CAS yet; TODO fix this
                c.systemProperty("test.repository_test_kit.skip_cas", "true");
            }
        })
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.gcs.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.gcs.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "repository_test_kit").put("bucket", bucket).put("base_path", basePath).build();
    }
}
