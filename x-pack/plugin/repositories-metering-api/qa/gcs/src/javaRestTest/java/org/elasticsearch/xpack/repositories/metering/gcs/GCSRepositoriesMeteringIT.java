/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class GCSRepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    private static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-gcs")
        .module("repositories-metering-api")
        .setting("gcs.client.repositories_metering.endpoint", () -> fixture.getAddress(), s -> USE_FIXTURE)
        .setting("gcs.client.repositories_metering.token_uri", () -> fixture.getAddress() + "/o/oauth2/token", s -> USE_FIXTURE)
        .apply(c -> {
            if (USE_FIXTURE) {
                c.keystore(
                    "gcs.client.repositories_metering.credentials_file",
                    Resource.fromString(() -> new String(TestUtils.createServiceAccount(random()), StandardCharsets.UTF_8))
                );
            } else {
                c.keystore(
                    "gcs.client.repositories_metering.credentials_file",
                    Resource.fromFile(PathUtils.get(System.getProperty("test.google.account")))
                );
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
    protected Map<String, String> repositoryLocation() {
        return Map.of("bucket", getProperty("test.gcs.bucket"), "base_path", getProperty("test.gcs.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = getProperty("test.gcs.bucket");
        final String basePath = getProperty("test.gcs.base_path");

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("gcs.client.repositories_metering.application_name", "updated").build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GetObject", "ListObjects");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("InsertObject");
    }
}
