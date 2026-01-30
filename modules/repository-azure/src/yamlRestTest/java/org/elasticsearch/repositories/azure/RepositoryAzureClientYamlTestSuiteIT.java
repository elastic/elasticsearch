/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpFixture;
import fixture.azure.MockAzureBlobStore;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.function.Predicate;

public class RepositoryAzureClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));
    private static final boolean USE_HTTPS_FIXTURE = USE_FIXTURE && ESTestCase.inFipsJvm() == false;
    // TODO when https://github.com/elastic/elasticsearch/issues/111532 addressed, use a HTTPS fixture in FIPS mode too

    private static final String AZURE_TEST_ACCOUNT = System.getProperty("test.azure.account");
    private static final String AZURE_TEST_CONTAINER = System.getProperty("test.azure.container");
    private static final String AZURE_TEST_KEY = System.getProperty("test.azure.key");
    private static final String AZURE_TEST_SASTOKEN = System.getProperty("test.azure.sas_token");
    private static final String AZURE_TEST_TENANT_ID = System.getProperty("test.azure.tenant_id");
    private static final String AZURE_TEST_CLIENT_ID = System.getProperty("test.azure.client_id");

    private static final AzureHttpFixture fixture = new AzureHttpFixture(
        USE_HTTPS_FIXTURE ? AzureHttpFixture.Protocol.HTTPS : USE_FIXTURE ? AzureHttpFixture.Protocol.HTTP : AzureHttpFixture.Protocol.NONE,
        AZURE_TEST_ACCOUNT,
        AZURE_TEST_CONTAINER,
        AZURE_TEST_TENANT_ID,
        AZURE_TEST_CLIENT_ID,
        decideAuthHeaderPredicate(),
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    private static Predicate<String> decideAuthHeaderPredicate() {
        if (Strings.hasText(AZURE_TEST_KEY) || Strings.hasText(AZURE_TEST_SASTOKEN)) {
            return AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_TEST_ACCOUNT);
        } else if (Strings.hasText(AZURE_TEST_TENANT_ID) && Strings.hasText(AZURE_TEST_CLIENT_ID)) {
            return AzureHttpFixture.WORK_IDENTITY_BEARER_TOKEN_PREDICATE;
        } else if (Strings.hasText(AZURE_TEST_TENANT_ID) || Strings.hasText(AZURE_TEST_CLIENT_ID)) {
            fail(null, "Both [test.azure.tenant_id] and [test.azure.client_id] must be set if either is set");
        }
        return AzureHttpFixture.MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE;
    }

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
        .systemProperty(
            "tests.azure.credentials.disable_instance_discovery",
            () -> "true",
            s -> USE_HTTPS_FIXTURE && Strings.hasText(AZURE_TEST_CLIENT_ID) && Strings.hasText(AZURE_TEST_TENANT_ID)
        )
        .systemProperty("AZURE_POD_IDENTITY_AUTHORITY_HOST", fixture::getMetadataAddress, s -> USE_FIXTURE)
        .systemProperty("AZURE_AUTHORITY_HOST", fixture::getOAuthTokenServiceAddress, s -> USE_HTTPS_FIXTURE)
        .systemProperty("AZURE_CLIENT_ID", () -> AZURE_TEST_CLIENT_ID, s -> Strings.hasText(AZURE_TEST_CLIENT_ID))
        .systemProperty("AZURE_TENANT_ID", () -> AZURE_TEST_TENANT_ID, s -> Strings.hasText(AZURE_TEST_TENANT_ID))
        .configFile("storage-azure/azure-federated-token", Resource.fromString(fixture.getFederatedToken()))
        .environment(
            nodeSpec -> USE_HTTPS_FIXTURE && Strings.hasText(AZURE_TEST_CLIENT_ID) && Strings.hasText(AZURE_TEST_TENANT_ID)
                ? Map.of("AZURE_FEDERATED_TOKEN_FILE", "${ES_PATH_CONF}/storage-azure/azure-federated-token")
                : Map.of()
        )
        .setting("thread_pool.repository_azure.max", () -> String.valueOf(randomIntBetween(1, 10)), s -> USE_FIXTURE)
        .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString(), s -> USE_HTTPS_FIXTURE)
        .systemProperty("javax.net.ssl.trustStoreType", () -> "jks", s -> USE_HTTPS_FIXTURE)
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
}
