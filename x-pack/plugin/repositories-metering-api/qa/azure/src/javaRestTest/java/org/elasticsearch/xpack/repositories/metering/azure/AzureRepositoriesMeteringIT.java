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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

public class AzureRepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));

    private static final String AZURE_TEST_ACCOUNT = System.getProperty("test.azure.account");
    private static final String AZURE_TEST_CONTAINER = System.getProperty("test.azure.container");
    private static final String AZURE_TEST_KEY = System.getProperty("test.azure.key");
    private static final String AZURE_TEST_SASTOKEN = System.getProperty("test.azure.sas_token");

    private static final TestTlsCertificate TEST_TLS_CERTIFICATE = TestTlsCertificate.generate("localhost");
    private static final TestTrustStore TEST_TRUST_STORE = new TestTrustStore(TEST_TLS_CERTIFICATE::getPemCertificateStream);

    private static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_FIXTURE ? AzureHttpFixture.Protocol.HTTPS : AzureHttpFixture.Protocol.NONE,
        TEST_TLS_CERTIFICATE,
        AZURE_TEST_ACCOUNT,
        AZURE_TEST_CONTAINER,
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT)
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
        .apply(builder -> TEST_TRUST_STORE.apply(builder, USE_FIXTURE))
        .build();

    @ClassRule(order = 1)
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(TEST_TRUST_STORE).around(cluster);

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
