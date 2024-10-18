/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.azure.AzureHttpFixture;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestTrustStore;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AzureRepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {
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
        decideAuthHeaderPredicate()
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

    private static final TestTrustStore trustStore = new TestTrustStore(
        () -> AzureHttpFixture.class.getResourceAsStream("azure-http-fixture.pem")
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-azure")
        .module("snapshot-repo-test-kit")
        .keystore("azure.client.repository_test_kit.account", AZURE_TEST_ACCOUNT)
        .keystore("azure.client.repository_test_kit.key", () -> AZURE_TEST_KEY, s -> Strings.hasText(AZURE_TEST_KEY))
        .keystore("azure.client.repository_test_kit.sas_token", () -> AZURE_TEST_SASTOKEN, s -> Strings.hasText(AZURE_TEST_SASTOKEN))
        .setting(
            "azure.client.repository_test_kit.endpoint_suffix",
            () -> "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + fixture.getAddress(),
            s -> USE_FIXTURE
        )
        .apply(c -> {
            if (USE_FIXTURE) {
                // test fixture does not support CAS yet; TODO fix this
                c.systemProperty("test.repository_test_kit.skip_cas", "true");
            }
        })
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
        .systemProperty("javax.net.ssl.trustStore", () -> trustStore.getTrustStorePath().toString(), s -> USE_HTTPS_FIXTURE)
        .systemProperty("javax.net.ssl.trustStoreType", () -> "jks", s -> USE_HTTPS_FIXTURE)
        .build();

    @ClassRule(order = 1)
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(trustStore).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected Settings repositorySettings() {
        final String container = System.getProperty("test.azure.container");
        assertThat(container, not(blankOrNullString()));

        final String basePath = System.getProperty("test.azure.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "repository_test_kit").put("container", container).put("base_path", basePath).build();
    }

    public void testClusterStats() throws IOException {
        registerRepository(randomIdentifier(), repositoryType(), true, repositorySettings());

        final var request = new Request(HttpGet.METHOD_NAME, "/_cluster/stats");
        final var response = client().performRequest(request);
        assertOK(response);

        final var objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("repositories.azure.count"), isSetIff(true));
        assertThat(objectPath.evaluate("repositories.azure.read_write"), isSetIff(true));

        assertThat(objectPath.evaluate("repositories.azure.uses_key_credentials"), isSetIff(Strings.hasText(AZURE_TEST_KEY)));
        assertThat(objectPath.evaluate("repositories.azure.uses_sas_token"), isSetIff(Strings.hasText(AZURE_TEST_SASTOKEN)));
        assertThat(
            objectPath.evaluate("repositories.azure.uses_default_credentials"),
            isSetIff((Strings.hasText(AZURE_TEST_SASTOKEN) || Strings.hasText(AZURE_TEST_KEY)) == false)
        );
        assertThat(
            objectPath.evaluate("repositories.azure.uses_managed_identity"),
            isSetIff(
                (Strings.hasText(AZURE_TEST_SASTOKEN) || Strings.hasText(AZURE_TEST_KEY) || Strings.hasText(AZURE_TEST_CLIENT_ID)) == false
            )
        );
        assertThat(objectPath.evaluate("repositories.azure.uses_workload_identity"), isSetIff(Strings.hasText(AZURE_TEST_CLIENT_ID)));
    }

    private static Matcher<Integer> isSetIff(boolean predicate) {
        return predicate ? equalTo(1) : nullValue(Integer.class);
    }
}
