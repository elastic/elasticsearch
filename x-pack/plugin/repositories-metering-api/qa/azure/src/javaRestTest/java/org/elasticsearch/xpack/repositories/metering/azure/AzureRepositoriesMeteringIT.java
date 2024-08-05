/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.azure;

import fixture.azure.AzureHttpFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

public class AzureRepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));
    private static final boolean USE_HTTPS_FIXTURE = USE_FIXTURE && ESTestCase.inFipsJvm() == false;

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
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT)
    );

    private static TestTrustStore trustStore = new TestTrustStore(
        () -> AzureHttpFixture.class.getResourceAsStream("azure-http-fixture.pem")
    );

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-azure")
        .module("repositories-metering-api")
        .keystore("azure.client.repositories_metering.account", AZURE_TEST_ACCOUNT)
        .keystore(
            "azure.client.repositories_metering.key",
            () -> AZURE_TEST_KEY,
            s -> AZURE_TEST_KEY != null && AZURE_TEST_KEY.isEmpty() == false
        )
        .keystore(
            "azure.client.repositories_metering.sas_token",
            () -> AZURE_TEST_SASTOKEN,
            s -> AZURE_TEST_SASTOKEN != null && AZURE_TEST_SASTOKEN.isEmpty() == false
        )
        .setting(
            "azure.client.repositories_metering.endpoint_suffix",
            () -> "ignored;DefaultEndpointsProtocol=https;BlobEndpoint=" + fixture.getAddress(),
            s -> USE_FIXTURE
        )
        .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString(), s -> USE_HTTPS_FIXTURE)
        .systemProperty("javax.net.ssl.trustStoreType", () -> "jks", s -> USE_HTTPS_FIXTURE)
        .build();

    @ClassRule(order = 1)
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(trustStore).around(cluster);

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Map<String, String> repositoryLocation() {
        return Map.of("container", getProperty("test.azure.container"), "base_path", getProperty("test.azure.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String container = getProperty("test.azure.container");

        final String basePath = getProperty("test.azure.base_path");

        return Settings.builder().put("client", "repositories_metering").put("container", container).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("azure.client.repositories_metering.max_retries", 5).build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GetBlob", "GetBlobProperties", "ListBlobs");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("PutBlob");
    }
}
