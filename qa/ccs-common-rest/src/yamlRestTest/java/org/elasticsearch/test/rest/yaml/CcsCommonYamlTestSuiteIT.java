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

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.test.rest.yaml.section.IsFalseAssertion;
import org.elasticsearch.test.rest.yaml.section.IsTrueAssertion;
import org.elasticsearch.test.rest.yaml.section.MatchAssertion;
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
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;

/**
 * This runner executes test suits against two clusters (a "write" (the remote) cluster and a
 * "search" cluster) connected via CCS.
 * The test runner maintains an additional client to the one provided by ESClientYamlSuiteTestCase
 * That client instance (and a corresponding client only used for administration) is running all API calls
 * defined in CCS_APIS against the "search" cluster, while all other operations like indexing are performed
 * using the client running against the "write" cluster.
 *
 */
@TimeoutSuite(millis = 15 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class CcsCommonYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final Logger logger = LogManager.getLogger(CcsCommonYamlTestSuiteIT.class);
    private static RestClient searchClient;
    private static RestClient adminSearchClient;
    private static List<HttpHost> clusterHosts;
    private static TestCandidateAwareClient searchYamlTestClient;
    // the remote cluster is the one we write index operations etc... to
    private static final String REMOTE_CLUSTER_NAME = "remote_cluster";

    private static LocalClusterConfigProvider commonClusterConfig = cluster -> cluster.module("x-pack-async-search")
        .module("aggregations")
        .module("analysis-common")
        .module("mapper-extras")
        .module("vector-tile")
        .module("x-pack-analytics")
        .module("x-pack-eql")
        .module("x-pack-sql")
        .setting("xpack.security.enabled", "false")
        // geohex_grid requires gold license
        .setting("xpack.license.self_generated.type", "trial")
        .feature(FeatureFlag.TIME_SERIES_MODE);

    private static ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name(REMOTE_CLUSTER_NAME)
        .nodes(2)
        .setting("node.roles", "[data,ingest,master]")
        .apply(commonClusterConfig)
        .build();

    private static ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local_cluster")
        .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
        .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote.remote_cluster.skip_unavailable", "false")
        .apply(commonClusterConfig)
        .build();

    @ClassRule
    // Use a RuleChain to ensure that remote cluster is started before local cluster
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    // the CCS api calls that we run against the "search" cluster in this test setup
    static final Set<String> CCS_APIS = Set.of(
        "search",
        "field_caps",
        "msearch",
        "scroll",
        "clear_scroll",
        "indices.resolve_index",
        "async_search.submit",
        "async_search.get",
        "async_search.status",
        "async_search.delete",
        "eql.search",
        "eql.get",
        "eql.get_status",
        "eql.delete",
        "sql.query",
        "sql.clear_cursor",
        "sql.translate",
        "open_point_in_time",
        "close_point_in_time"
    );

    @Override
    protected String getTestRestCluster() {
        return remoteCluster.getHttpAddresses();
    }

    /**
     * initialize the search client and an additional administration client and check for an established connection
     */
    @Before
    public void initSearchClient() throws IOException {
        if (searchClient == null) {
            assert adminSearchClient == null;
            assert clusterHosts == null;

            String[] stringUrls = localCluster.getHttpAddresses().split(",");
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
            searchClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
            adminSearchClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));

            searchYamlTestClient = new TestCandidateAwareClient(getRestSpec(), searchClient, hosts, this::getClientBuilderWithSniffedHosts);

            // check that we have an established CCS connection
            Request request = new Request("GET", "_remote/info");
            Response response = adminSearchClient.performRequest(request);
            assertOK(response);
            ObjectPath responseObject = ObjectPath.createFromResponse(response);
            assertNotNull(responseObject.evaluate(REMOTE_CLUSTER_NAME));
            assertNull(responseObject.evaluate(REMOTE_CLUSTER_NAME + ".cluster_credentials"));
            logger.info("Established connection to remote cluster [" + REMOTE_CLUSTER_NAME + "]");
        }

        assert searchClient != null;
        assert adminSearchClient != null;
        assert clusterHosts != null;

        searchYamlTestClient.setTestCandidate(getTestCandidate());
    }

    public CcsCommonYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) throws IOException {
        super(rewrite(testCandidate));
    }

    /**
     * we need to rewrite a few "match" sections in order to change the expected index name values
     * to include the remote cluster prefix
     */
    static ClientYamlTestCandidate rewrite(ClientYamlTestCandidate clientYamlTestCandidate) {
        ClientYamlTestSection testSection = clientYamlTestCandidate.getTestSection();
        List<ExecutableSection> executableSections = testSection.getExecutableSections();
        List<ExecutableSection> modifiedExecutableSections = new ArrayList<>();
        String lastAPIDoSection = "";
        for (ExecutableSection section : executableSections) {
            ExecutableSection rewrittenSection = section;
            if (section instanceof MatchAssertion matchSection) {
                Object modifiedExpectedValue = ((MatchAssertion) section).getExpectedValue();
                if (matchSection.getField().endsWith("_index") || matchSection.getField().contains("fields._index")) {
                    modifiedExpectedValue = rewriteExpectedIndexValue(matchSection.getExpectedValue());
                }
                if (lastAPIDoSection.equals("indices.resolve_index") && matchSection.getField().endsWith("name")) {
                    // modify " indices.resolve_index" expected index names
                    modifiedExpectedValue = rewriteExpectedIndexValue(matchSection.getExpectedValue());
                }
                if (lastAPIDoSection.equals("field_caps") && matchSection.getField().endsWith("indices")) {
                    modifiedExpectedValue = rewriteExpectedIndexValue(matchSection.getExpectedValue());
                }
                rewrittenSection = new MatchAssertion(matchSection.getLocation(), matchSection.getField(), modifiedExpectedValue);
            } else if (section instanceof IsFalseAssertion falseAssertion) {
                if ((lastAPIDoSection.startsWith("async_") || lastAPIDoSection.equals("search"))
                    && ((IsFalseAssertion) section).getField().endsWith("_clusters")) {
                    // in ccs scenarios, the response "_cluster" section will be there
                    rewrittenSection = new IsTrueAssertion(falseAssertion.getLocation(), falseAssertion.getField());
                }
            } else if (section instanceof DoSection) {
                lastAPIDoSection = ((DoSection) section).getApiCallSection().getApi();
                if (lastAPIDoSection.equals("msearch")) {
                    // modify "msearch" body sections so the "index" part is targeting the remote cluster
                    DoSection doSection = ((DoSection) section);
                    List<Map<String, Object>> bodies = doSection.getApiCallSection().getBodies();
                    for (Map<String, Object> body : bodies) {
                        if (body.containsKey("index")) {
                            String modifiedIndex = REMOTE_CLUSTER_NAME + ":" + body.get("index");
                            body.put("index", modifiedIndex);
                        } else if (body.containsKey("query") && body.containsKey("pit")) {
                            // search/350_point_in_time/msearch uses _index in a match query
                            @SuppressWarnings("unchecked")
                            final var query = (Map<String, Object>) body.get("query");
                            if (query.containsKey("match")) {
                                @SuppressWarnings("unchecked")
                                final var match = (Map<String, Object>) query.get("match");
                                if (match.containsKey("_index")) {
                                    match.put("_index", REMOTE_CLUSTER_NAME + ":" + match.get("_index"));
                                }
                            }
                        }
                    }
                } else if (lastAPIDoSection.equals("sql.query") || lastAPIDoSection.equals("sql.translate")) {
                    DoSection doSection = ((DoSection) section);
                    List<Map<String, Object>> bodies = doSection.getApiCallSection().getBodies();
                    for (Map<String, Object> body : bodies) {
                        if (body.containsKey("query")) {
                            final String query = (String) body.get("query");
                            // Prefix the index name after FROM with the remote cluster alias
                            // Split and join the old query string to take care of any excessive whitespaces
                            final String rewrittenQuery = Strings.arrayToDelimitedString(query.split("\\s+"), " ")
                                .replace("FROM ", "FROM " + REMOTE_CLUSTER_NAME + ":");
                            body.put("query", rewrittenQuery);
                        }
                    }
                }
            }
            modifiedExecutableSections.add(rewrittenSection);
        }
        return new ClientYamlTestCandidate(
            clientYamlTestCandidate.getRestTestSuite(),
            new ClientYamlTestSection(
                testSection.getLocation(),
                testSection.getName(),
                testSection.getPrerequisiteSection(),
                modifiedExecutableSections
            )
        );
    }

    /**
     * add the remote cluster prefix to either a single index name or a list of expected index names
     */
    private static Object rewriteExpectedIndexValue(Object expectedValue) {
        if (expectedValue instanceof String) {
            return REMOTE_CLUSTER_NAME + ":" + expectedValue;
        }
        if (expectedValue instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> expectedValues = (List<String>) expectedValue;
            return expectedValues.stream().map(s -> REMOTE_CLUSTER_NAME + ":" + s).toList();
        }
        throw new IllegalArgumentException("Either String or List<String> expected");
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

    static class TestCandidateAwareClient extends ClientYamlTestClient {
        private ClientYamlTestCandidate testCandidate;

        TestCandidateAwareClient(
            ClientYamlSuiteRestSpec restSpec,
            RestClient restClient,
            List<HttpHost> hosts,
            CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes
        ) {
            super(restSpec, restClient, hosts, clientBuilderWithSniffedNodes);
        }

        public void setTestCandidate(ClientYamlTestCandidate testCandidate) {
            this.testCandidate = testCandidate;
        }

        // we overwrite this method so the search client can modify the index names by prefixing them with the
        // remote cluster name before sending the requests
        public ClientYamlTestResponse callApi(
            String apiName,
            Map<String, String> params,
            HttpEntity entity,
            Map<String, String> headers,
            NodeSelector nodeSelector,
            BiPredicate<ClientYamlSuiteRestApi, ClientYamlSuiteRestApi.Path> pathPredicate
        ) throws IOException {
            // on request, we need to replace index specifications by prefixing the remote cluster
            if (shouldReplaceIndexWithRemote(apiName)) {
                String parameterName = "index";
                if (apiName.equals("indices.resolve_index")) {
                    // in this specific api, the index parameter is called "name"
                    parameterName = "name";
                }
                String originalIndices = params.get(parameterName);
                String expandedIndices = REMOTE_CLUSTER_NAME + ":*";
                if (originalIndices != null && (originalIndices.isEmpty() == false)) {
                    String[] indices = originalIndices.split(",");
                    List<String> newIndices = new ArrayList<>();
                    for (String indexName : indices) {
                        newIndices.add(REMOTE_CLUSTER_NAME + ":" + indexName);
                    }
                    expandedIndices = String.join(",", newIndices);
                }
                params.put(parameterName, String.join(",", expandedIndices));
            }
            return super.callApi(apiName, params, entity, headers, nodeSelector, pathPredicate);
        }

        private boolean shouldReplaceIndexWithRemote(String apiName) {
            if (apiName.equals("scroll")
                || apiName.equals("clear_scroll")
                || apiName.equals("async_search.get")
                || apiName.equals("async_search.delete")
                || apiName.equals("async_search.status")
                || apiName.equals("eql.get")
                || apiName.equals("eql.get_status")
                || apiName.equals("eql.delete")
                || apiName.equals("sql.query")
                || apiName.equals("sql.clear_cursor")
                || apiName.equals("sql.translate")
                || apiName.equals("close_point_in_time")) {
                return false;
            }

            if (apiName.equals("search") || apiName.equals("msearch") || apiName.equals("async_search.submit")) {
                final String testCandidateTestPath = testCandidate.getTestPath();
                if (testCandidateTestPath.startsWith("search/350_point_in_time")
                    || testCandidateTestPath.equals("async_search/20-with-poin-in-time/Async search with point in time")) {
                    return false;
                }
            }
            return true;
        }
    }
}
