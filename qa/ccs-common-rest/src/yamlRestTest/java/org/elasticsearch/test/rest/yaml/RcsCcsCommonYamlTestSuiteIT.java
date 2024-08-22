/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.CcsCommonYamlTestSuiteIT.TestCandidateAwareClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.test.rest.yaml.CcsCommonYamlTestSuiteIT.CCS_APIS;
import static org.elasticsearch.test.rest.yaml.CcsCommonYamlTestSuiteIT.rewrite;

/**
 * This runner executes test suits against two clusters (a "write" (the remote) cluster and a
 * "search" cluster) connected via CCS.
 * The test runner maintains an additional client to the one provided by ESClientYamlSuiteTestCase
 * That client instance (and a corresponding client only used for administration) is running all API calls
 * defined in CCS_APIS against the "search" cluster, while all other operations like indexing are performed
 * using the client running against the "write" cluster.
 *
 */
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class RcsCcsCommonYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final Logger logger = LogManager.getLogger(RcsCcsCommonYamlTestSuiteIT.class);
    private static RestClient searchClient;
    private static RestClient adminSearchClient;
    private static List<HttpHost> clusterHosts;
    private static TestCandidateAwareClient searchYamlTestClient;
    // the remote cluster is the one we write index operations etc... to
    private static final String REMOTE_CLUSTER_NAME = "remote_cluster";
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    private static LocalClusterConfigProvider commonClusterConfig = cluster -> cluster.module("x-pack-async-search")
        .module("aggregations")
        .module("mapper-extras")
        .module("analysis-common")
        .module("vector-tile")
        .module("x-pack-analytics")
        .module("x-pack-eql")
        .module("x-pack-sql")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.remote_cluster_server.ssl.enabled", "false")
        .setting("xpack.security.remote_cluster_client.ssl.enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .user("test_admin", "x-pack-test-password");

    private static ElasticsearchCluster fulfillingCluster = ElasticsearchCluster.local()
        .name(REMOTE_CLUSTER_NAME)
        .nodes(2)
        .setting("node.roles", "[data,ingest,master]")
        .setting("remote_cluster_server.enabled", "true")
        .setting("remote_cluster.port", "0")
        .apply(commonClusterConfig)
        .build();

    private static ElasticsearchCluster queryCluster = ElasticsearchCluster.local()
        .name("query-cluster")
        .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
        .setting("cluster.remote.connections_per_cluster", "1")
        .apply(commonClusterConfig)
        .keystore("cluster.remote." + REMOTE_CLUSTER_NAME + ".credentials", () -> {
            if (API_KEY_MAP_REF.get() == null) {
                try {
                    API_KEY_MAP_REF.set(createCrossClusterAccessApiKey());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return (String) API_KEY_MAP_REF.get().get("encoded");
        })
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("remote_search_user", "x-pack-test-password", "remote_search_role", false)
        .build();

    private static Map<String, Object> createCrossClusterAccessApiKey() throws IOException {
        assert fulfillingCluster != null;
        final var createApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "cross_cluster_access_key",
              "access": {
                  "search": [
                    {
                      "names": ["*"],
                      "allow_restricted_indices": true
                    }
                  ]
              }
            }""");
        createApiKeyRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray())))
        );

        final int numberOfFcNodes = fulfillingCluster.getHttpAddresses().split(",").length;
        final String url = fulfillingCluster.getHttpAddress(randomIntBetween(0, numberOfFcNodes - 1));
        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(url.substring(0, portSeparator), Integer.parseInt(url.substring(portSeparator + 1)), "http");
        RestClientBuilder builder = RestClient.builder(httpHost);
        doConfigureClient(builder, Settings.EMPTY);
        builder.setStrictDeprecationMode(true);
        try (RestClient fulfillingClusterClient = builder.build()) {
            final Response createApiKeyResponse = fulfillingClusterClient.performRequest(createApiKeyRequest);
            assertOK(createApiKeyResponse);
            return responseAsMap(createApiKeyResponse);
        }
    }

    @ClassRule
    // Use a RuleChain to ensure that remote cluster is started before local cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected String getTestRestCluster() {
        return fulfillingCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveSecurityIndicesUponCompletion() {
        return true;
    }

    /**
     * initialize the search client and an additional administration client and check for an established connection
     */
    @Before
    public void initSearchClient() throws IOException {
        if (searchClient == null) {
            assert adminSearchClient == null;
            assert clusterHosts == null;

            String[] stringUrls = queryCluster.getHttpAddresses().split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
                hosts.add(buildHttpHost(host, port));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST search clients against {}", clusterHosts);
            searchClient = buildClient(
                Settings.builder()
                    .put(
                        ThreadContext.PREFIX + ".Authorization",
                        basicAuthHeaderValue("remote_search_user", new SecureString("x-pack-test-password".toCharArray()))
                    )
                    .build(),
                clusterHosts.toArray(new HttpHost[clusterHosts.size()])
            );
            adminSearchClient = buildClient(
                Settings.builder()
                    .put(
                        ThreadContext.PREFIX + ".Authorization",
                        basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()))
                    )
                    .build(),
                clusterHosts.toArray(new HttpHost[clusterHosts.size()])
            );

            searchYamlTestClient = new TestCandidateAwareClient(getRestSpec(), searchClient, hosts, this::getClientBuilderWithSniffedHosts);

            configureRemoteCluster();
            // check that we have an established CCS connection
            Request request = new Request("GET", "_remote/info");
            Response response = adminSearchClient.performRequest(request);
            assertOK(response);
            ObjectPath responseObject = ObjectPath.createFromResponse(response);
            assertNotNull(responseObject.evaluate(REMOTE_CLUSTER_NAME));
            assertEquals("::es_redacted::", responseObject.evaluate(REMOTE_CLUSTER_NAME + ".cluster_credentials"));
            logger.info("Established connection to remote cluster [" + REMOTE_CLUSTER_NAME + "]");
        }

        assert searchClient != null;
        assert adminSearchClient != null;
        assert clusterHosts != null;

        searchYamlTestClient.setTestCandidate(getTestCandidate());
    }

    private static void configureRemoteCluster() throws IOException {
        final Settings.Builder builder = Settings.builder();
        builder.put("cluster.remote." + REMOTE_CLUSTER_NAME + ".skip_unavailable", "false");
        if (randomBoolean()) {
            builder.put("cluster.remote." + REMOTE_CLUSTER_NAME + ".mode", "proxy")
                .put("cluster.remote." + REMOTE_CLUSTER_NAME + ".proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        } else {
            builder.put("cluster.remote." + REMOTE_CLUSTER_NAME + ".mode", "sniff")
                .putList("cluster.remote." + REMOTE_CLUSTER_NAME + ".seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        }
        final Settings remoteClusterSettings = builder.build();

        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"persistent\":" + Strings.toString(remoteClusterSettings) + "}");
        Response response = adminSearchClient.performRequest(request);
        assertOK(response);
    }

    public RcsCcsCommonYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) throws IOException {
        super(rewrite(testCandidate));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        final Set<String> nodesVersions,
        final TestFeatureService testFeatureService,
        final Set<String> osSet
    ) {
        try {
            // Ensure the test specific initialization is run by calling it explicitly (@Before annotations on base-derived class may
            // be called in a different order)
            initSearchClient();
            // Reconcile and provide unified features, os, version(s), based on both clientYamlTestClient and searchYamlTestClient
            var searchOs = readOsFromNodesInfo(adminSearchClient);
            var searchNodeVersions = readVersionsFromNodesInfo(adminSearchClient);
            var semanticNodeVersions = searchNodeVersions.stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
            final TestFeatureService searchTestFeatureService = createTestFeatureService(
                getClusterStateFeatures(adminSearchClient),
                semanticNodeVersions
            );
            final TestFeatureService combinedTestFeatureService = (featureId, any) -> {
                boolean adminFeature = testFeatureService.clusterHasFeature(featureId, any);
                boolean searchFeature = searchTestFeatureService.clusterHasFeature(featureId, any);
                return any ? adminFeature || searchFeature : adminFeature && searchFeature;
            };

            final Set<String> combinedOsSet = Stream.concat(osSet.stream(), Stream.of(searchOs)).collect(Collectors.toSet());
            final Set<String> combinedNodeVersions = Stream.concat(nodesVersions.stream(), searchNodeVersions.stream())
                .collect(Collectors.toSet());

            return new ClientYamlTestExecutionContext(
                clientYamlTestCandidate,
                clientYamlTestClient,
                randomizeContentType(),
                combinedNodeVersions,
                combinedTestFeatureService,
                combinedOsSet
            ) {
                // depending on the API called, we either return the client running against the "write" or the "search" cluster here
                protected ClientYamlTestClient clientYamlTestClient(String apiName) {
                    if (CCS_APIS.contains(apiName)) {
                        return searchYamlTestClient;
                    } else {
                        return super.clientYamlTestClient(apiName);
                    }
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterClass
    public static void closeSearchClients() throws IOException {
        try {
            IOUtils.close(searchClient, adminSearchClient);
        } finally {
            clusterHosts = null;
        }
    }
}
