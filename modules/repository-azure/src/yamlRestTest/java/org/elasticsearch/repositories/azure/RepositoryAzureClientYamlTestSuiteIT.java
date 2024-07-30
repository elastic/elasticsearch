/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class RepositoryAzureClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));
    private static final String AZURE_TEST_ACCOUNT = System.getProperty("test.azure.account");
    private static final String AZURE_TEST_CONTAINER = System.getProperty("test.azure.container");
    private static final String AZURE_TEST_KEY = System.getProperty("test.azure.key");
    private static final String AZURE_TEST_SASTOKEN = System.getProperty("test.azure.sas_token");
    private static final String AZURE_TEST_TENANT_ID = System.getProperty("test.azure.tenant_id");
    private static final String AZURE_TEST_CLIENT_ID = System.getProperty("test.azure.client_id");

    private static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_FIXTURE ? AzureHttpFixture.Protocol.HTTPS : AzureHttpFixture.Protocol.NONE,
        AZURE_TEST_ACCOUNT,
        AZURE_TEST_CONTAINER,
        AZURE_TEST_TENANT_ID,
        AZURE_TEST_CLIENT_ID,
        Strings.hasText(AZURE_TEST_KEY) || Strings.hasText(AZURE_TEST_SASTOKEN)
            ? AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT)
            : AzureHttpFixture.MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE
    );

    private static TestTrustStore trustStore = new TestTrustStore(
        () -> AzureHttpFixture.class.getResourceAsStream("azure-http-fixture.pem")
    );

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-azure")
        .keystore("azure.client.integration_test.account", AZURE_TEST_ACCOUNT)
        .keystore(
            "azure.client.integration_test.key",
            () -> AZURE_TEST_KEY,
            s -> AZURE_TEST_KEY != null && AZURE_TEST_KEY.isEmpty() == false
        )
        .keystore(
            "azure.client.integration_test.sas_token",
            () -> AZURE_TEST_SASTOKEN,
            s -> AZURE_TEST_SASTOKEN != null && AZURE_TEST_SASTOKEN.isEmpty() == false
        )
        .setting(
            "azure.client.integration_test.endpoint_suffix",
            () -> "ignored;DefaultEndpointsProtocol=https;BlobEndpoint=" + fixture.getAddress(),
            s -> USE_FIXTURE
        )
        .systemProperty("AZURE_POD_IDENTITY_AUTHORITY_HOST", () -> fixture.getMetadataAddress(), s -> USE_FIXTURE)
        .systemProperty("AZURE_AUTHORITY_HOST", () -> fixture.getOAuthTokenServiceAddress(), s -> USE_FIXTURE)
        .systemProperty("AZURE_CLIENT_ID", () -> AZURE_TEST_CLIENT_ID, s -> notNullOrEmpty(AZURE_TEST_CLIENT_ID))
        .systemProperty("AZURE_TENANT_ID", () -> AZURE_TEST_TENANT_ID, s -> notNullOrEmpty(AZURE_TEST_TENANT_ID))
        .systemProperty(
            "AZURE_FEDERATED_TOKEN_FILE",
            () -> fixture.getFederatedTokenPath().toString(),
            s -> USE_FIXTURE && notNullOrEmpty(AZURE_TEST_CLIENT_ID) && notNullOrEmpty(AZURE_TEST_TENANT_ID)
        )
        .setting("thread_pool.repository_azure.max", () -> String.valueOf(randomIntBetween(1, 10)), s -> USE_FIXTURE)
        .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString(), s -> USE_FIXTURE)
        .build();

    @ClassRule(order = 1)
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(trustStore).around(cluster);

    public RepositoryAzureClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static boolean notNullOrEmpty(String s) {
        return s != null && false == s.isEmpty();
    }
}
