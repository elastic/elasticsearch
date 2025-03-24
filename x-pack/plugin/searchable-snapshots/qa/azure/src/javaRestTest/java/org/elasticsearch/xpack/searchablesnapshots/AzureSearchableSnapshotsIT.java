/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import fixture.azure.AzureHttpFixture;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class AzureSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));
    private static final boolean USE_HTTPS_FIXTURE = USE_FIXTURE && ESTestCase.inFipsJvm() == false;
    // TODO when https://github.com/elastic/elasticsearch/issues/111532 addressed, use a HTTPS fixture in FIPS mode too

    private static final String AZURE_TEST_ACCOUNT = System.getProperty("test.azure.account");
    private static final String AZURE_TEST_CONTAINER = System.getProperty("test.azure.container");
    private static final String AZURE_TEST_KEY = System.getProperty("test.azure.key");
    private static final String AZURE_TEST_SASTOKEN = System.getProperty("test.azure.sas_token");

    private static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_HTTPS_FIXTURE ? AzureHttpFixture.Protocol.HTTPS : USE_FIXTURE ? AzureHttpFixture.Protocol.HTTP : AzureHttpFixture.Protocol.NONE,
        AZURE_TEST_ACCOUNT,
        AZURE_TEST_CONTAINER,
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT),
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    private static TestTrustStore trustStore = new TestTrustStore(
        () -> AzureHttpFixture.class.getResourceAsStream("azure-http-fixture.pem")
    );

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-azure")
        .module("searchable-snapshots")
        .module("lang-painless")
        .keystore("azure.client.searchable_snapshots.account", AZURE_TEST_ACCOUNT)
        .keystore(
            "azure.client.searchable_snapshots.key",
            () -> AZURE_TEST_KEY,
            s -> AZURE_TEST_KEY != null && AZURE_TEST_KEY.isEmpty() == false
        )
        .keystore(
            "azure.client.searchable_snapshots.sas_token",
            () -> AZURE_TEST_SASTOKEN,
            s -> AZURE_TEST_SASTOKEN != null && AZURE_TEST_SASTOKEN.isEmpty() == false
        )
        .setting(
            "azure.client.searchable_snapshots.endpoint_suffix",
            () -> "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + fixture.getAddress(),
            s -> USE_FIXTURE
        )
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString(), s -> USE_HTTPS_FIXTURE)
        .systemProperty("javax.net.ssl.trustStoreType", () -> "jks", s -> USE_HTTPS_FIXTURE)
        .build();

    @ClassRule(order = 1)
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(trustStore).around(cluster);

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds
            // this ensures that we don't retry the test requests when azure is misbehaving
            // (the azure sdk timeout is 60s) see #87389
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String writeRepositoryType() {
        return "azure";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String container = System.getProperty("test.azure.container");
        assertThat(container, not(blankOrNullString()));

        final String basePath = System.getProperty("test.azure.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("container", container).put("base_path", basePath).build();
    }
}
