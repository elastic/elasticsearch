/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_POLICIES;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.COMPLETION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.ENABLE_FORK_FOR_REMOTE_INDICES_V2;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.ENABLE_LOOKUP_JOIN_ON_REMOTE;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS_SUPPORTS_REMOTE;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_PLANNING_V1;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.METADATA_FIELDS_REMOTE_TEST;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.METRICS_INFO_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.RERANK;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.UNMAPPED_FIELDS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.VIEWS_WITH_NO_BRANCHING;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * This suite loads the data into either the local cluster or the remote cluster, then run spec tests with CCQ.
 * TODO: Some spec tests prevents us from splitting data across multiple shards/indices/clusters
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterSpecIT extends EsqlSpecTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster(LOGGING_CLUSTER_SETTINGS);
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster, LOGGING_CLUSTER_SETTINGS);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static TestFeatureService remoteFeaturesService;
    private static RestClient remoteClusterClient;
    private static DataLocation dataLocation = null;

    private static final Set<String> LOCAL_ONLY_INFERENCE_CAPABILITIES = Set.of(
        RERANK.capabilityName(),
        COMPLETION.capabilityName(),
        TEXT_EMBEDDING_FUNCTION.capabilityName()
    );

    private static final RequestOptions DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else {
                for (String warning : warnings) {
                    if ("Parameter [default_metric] is deprecated and will be removed in a future version".equals(warning) == false) {
                        return true;
                    }
                }
                return false;
            }
        })
        .build();

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    public MultiClusterSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, convertToRemoteIndices(testCase), instructions);
    }

    // TODO: think how to handle this better
    public static final Set<String> NO_REMOTE_LOOKUP_JOIN_TESTS = Set.of(
        // Lookup join after STATS is not supported in CCS yet
        "StatsAndLookupIPAndMessageFromIndex",
        "JoinMaskingRegex",
        "StatsAndLookupIPFromIndex",
        "StatsAndLookupMessageFromIndex",
        "MvJoinKeyOnTheLookupIndexAfterStats",
        "MvJoinKeyOnFromAfterStats",
        // Lookup join after SORT is not supported in CCS yet
        "NullifiedJoinKeyToPurgeTheJoin",
        "SortBeforeAndAfterJoin",
        "SortEvalBeforeLookup",
        "SortBeforeAndAfterMultipleJoinAndMvExpand",
        "LookupJoinAfterTopNAndRemoteEnrich",
        "LookupJoinOnTwoFieldsAfterTop",
        "LookupJoinOnTwoFieldsMultipleTimes",
        // Lookup join after LIMIT is not supported in CCS yet
        "LookupJoinAfterLimitAndRemoteEnrich",
        "LookupJoinExpressionAfterLimitAndRemoteEnrich",
        "LookupJoinWithSemanticFilterDeduplicationComplex",
        // Lookup join after FORK is not support in CCS yet
        "ForkBeforeLookupJoin"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        boolean remoteMetadata = testCase.requiredCapabilities.contains(METADATA_FIELDS_REMOTE_TEST.capabilityName());
        if (remoteMetadata) {
            // remove the capability from the test to enable it
            testCase.requiredCapabilities = testCase.requiredCapabilities.stream()
                .filter(c -> c.equals("metadata_fields_remote_test") == false)
                .toList();
        }
        // Check all capabilities on the local cluster first.
        super.shouldSkipTest(testName);

        // Filter out capabilities that are required only on the local cluster and then check the remaining on the remote cluster.
        List<String> remoteCapabilities = testCase.requiredCapabilities.stream()
            .filter(c -> LOCAL_ONLY_INFERENCE_CAPABILITIES.contains(c) == false)
            .toList();
        checkCapabilities(remoteClusterClient(), remoteFeaturesService(), testName, remoteCapabilities);

        // Do not run tests including "METADATA _index" unless marked with metadata_fields_remote_test,
        // because they may produce inconsistent results with multiple clusters.
        assumeFalse("can't test with _index metadata", (remoteMetadata == false) && hasIndexMetadata(testCase.query));
        Version oldVersion = Version.min(Clusters.localClusterVersion(), Clusters.remoteClusterVersion());
        assumeTrue("Test " + testName + " is skipped on " + oldVersion, isEnabled(testName, instructions, oldVersion));
        if (testCase.requiredCapabilities.contains(INLINE_STATS.capabilityName())
            || testCase.requiredCapabilities.contains(JOIN_PLANNING_V1.capabilityName())) {
            assumeTrue(
                "INLINE STATS in CCS not supported for this version",
                hasCapabilities(adminClient(), List.of(INLINE_STATS_SUPPORTS_REMOTE.capabilityName()))
            );
            assumeTrue(
                "INLINE STATS in CCS not supported for this version",
                hasCapabilities(remoteClusterClient(), List.of(INLINE_STATS_SUPPORTS_REMOTE.capabilityName()))
            );
        }
        if (testCase.requiredCapabilities.contains(JOIN_LOOKUP_V12.capabilityName())) {
            assumeTrue(
                "LOOKUP JOIN not yet supported in CCS",
                hasCapabilities(adminClient(), List.of(ENABLE_LOOKUP_JOIN_ON_REMOTE.capabilityName()))
            );
        }
        // Unmapped fields require a correct capability response from every cluster, which isn't currently implemented.
        assumeFalse("UNMAPPED FIELDS not yet supported in CCS", testCase.requiredCapabilities.contains(UNMAPPED_FIELDS.capabilityName()));
        // Tests that use capabilities not supported in CCS
        assumeFalse(
            "This syntax is not supported with remote LOOKUP JOIN",
            NO_REMOTE_LOOKUP_JOIN_TESTS.stream().anyMatch(testName::contains)
        );
        // Tests that do SORT before LOOKUP JOIN - not supported in CCS
        assumeFalse("LOOKUP JOIN after SORT not yet supported in CCS", testName.contains("OnTheCoordinator"));

        if (testCase.requiredCapabilities.contains(FORK_V9.capabilityName())) {
            assumeTrue(
                "FORK not yet supported with CCS",
                hasCapabilities(adminClient(), List.of(ENABLE_FORK_FOR_REMOTE_INDICES_V2.capabilityName()))
            );
        }

        // MultiCluster CCS does not yet support VIEWS, due to rewriting FROM name to FROM *:name
        assumeFalse("VIEWS not yet supported in CCS", testCase.requiredCapabilities.contains(VIEWS_WITH_NO_BRANCHING.capabilityName()));
        assumeFalse("VIEWS not yet supported in CCS", testCase.requiredCapabilities.contains(VIEWS_WITH_BRANCHING.capabilityName()));

        if (testCase.requiredCapabilities.contains(METRICS_INFO_COMMAND.capabilityName())) {
            assumeFalse(
                "METRICS_INFO not supported in CCS",
                hasCapabilities(remoteClusterClient(), List.of(METRICS_INFO_COMMAND.capabilityName()))
            );
        }
    }

    private TestFeatureService remoteFeaturesService() throws IOException {
        if (remoteFeaturesService == null) {
            var remoteNodeVersions = readVersionsFromNodesInfo(remoteClusterClient());
            remoteFeaturesService = createTestFeatureService(
                getClusterStateFeatures(remoteClusterClient()),
                fromSemanticVersions(remoteNodeVersions)
            );
        }
        return remoteFeaturesService;
    }

    private RestClient remoteClusterClient() throws IOException {
        if (remoteClusterClient == null) {
            HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
            remoteClusterClient = super.buildClient(restAdminSettings(), remoteHosts);
        }
        return remoteClusterClient;
    }

    @AfterClass
    public static void closeRemoveFeaturesService() throws IOException {
        IOUtils.close(remoteClusterClient);
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] localHosts) throws IOException {
        RestClient localClient = super.buildClient(settings, localHosts);
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        RestClient remoteClient = super.buildClient(settings, remoteHosts);
        return twoClients(localClient, remoteClient);
    }

    // These indices are used in metadata tests so we want them on remote only for consistency
    public static final List<String> METADATA_INDICES = List.of("employees", "apps", "ul_logs");

    // These are lookup indices, we want them on both remotes and locals
    public static final Set<String> LOOKUP_INDICES = CSV_DATASET_MAP.values()
        .stream()
        .filter(td -> td.settingFileName() != null && td.settingFileName().equals("lookup-settings.json"))
        .map(CsvTestsDataLoader.TestDataset::indexName)
        .collect(toSet());

    public static final Set<String> LOOKUP_ENDPOINTS = LOOKUP_INDICES.stream().map(i -> "/" + i + "/_bulk").collect(toSet());

    public static final Set<String> ENRICH_ENDPOINTS = ENRICH_POLICIES.stream().map(p -> "/" + p.index() + "/_bulk").collect(toSet());

    /**
     * Creates a new mock client that dispatches every request to both the local and remote clusters, excluding _bulk, _query,
     *  and _inference requests :
     * - '_bulk' requests are randomly sent to either the local or remote cluster to populate data. Some spec tests, such as AVG,
     *   prevent the splitting of bulk requests.
     * - '_query' requests are dispatched to the local cluster only, as we are testing cross-cluster queries.
     * - '_inference' requests are dispatched to the local cluster only, as inference endpoints are not available on remote clusters.
     */
    static RestClient twoClients(RestClient localClient, RestClient remoteClient) throws IOException {
        RestClient twoClients = mock(RestClient.class);
        assertNotNull("data location was set", dataLocation);
        // write to a single cluster for now due to the precision of some functions such as avg and tests related to updates
        final RestClient bulkClient = dataLocation == DataLocation.REMOTE_ONLY ? remoteClient : randomFrom(localClient, remoteClient);
        when(twoClients.performRequest(any())).then(invocation -> {
            Request request = invocation.getArgument(0);
            String endpoint = request.getEndpoint();
            if (endpoint.startsWith("/_query")) {
                return localClient.performRequest(request);
            } else if (endpoint.startsWith("/_inference")) {
                return localClient.performRequest(request);
            } else if (endpoint.endsWith("/_bulk") && METADATA_INDICES.stream().anyMatch(i -> endpoint.equals("/" + i + "/_bulk"))) {
                return remoteClient.performRequest(request);
            } else if (endpoint.endsWith("/_bulk")
                && ENRICH_ENDPOINTS.contains(endpoint) == false
                && LOOKUP_ENDPOINTS.contains(endpoint) == false) {
                    return bulkClient.performRequest(request);
                } else {
                    request.setOptions(DEPRECATED_DEFAULT_METRIC_WARNING_HANDLER);
                    Request[] clones = cloneRequests(request, 2);
                    Response resp1 = remoteClient.performRequest(clones[0]);
                    Response resp2 = localClient.performRequest(clones[1]);
                    assertEquals(resp1.getStatusLine().getStatusCode(), resp2.getStatusLine().getStatusCode());
                    return resp2;
                }
        });
        doAnswer(invocation -> {
            IOUtils.close(localClient, remoteClient);
            return null;
        }).when(twoClients).close();
        return twoClients;
    }

    enum DataLocation {
        REMOTE_ONLY,
        ANY_CLUSTER
    }

    static Request[] cloneRequests(Request orig, int numClones) throws IOException {
        Request[] clones = new Request[numClones];
        for (int i = 0; i < clones.length; i++) {
            clones[i] = new Request(orig.getMethod(), orig.getEndpoint());
            clones[i].addParameters(orig.getParameters());
            clones[i].setOptions(orig.getOptions());
        }
        HttpEntity entity = orig.getEntity();
        if (entity != null) {
            byte[] bytes = entity.getContent().readAllBytes();
            entity.getContent().close();
            for (Request clone : clones) {
                ByteArrayInputStream cloneInput = new ByteArrayInputStream(bytes);
                HttpEntity cloneEntity = spy(entity);
                when(cloneEntity.getContent()).thenReturn(cloneInput);
                clone.setEntity(cloneEntity);
            }
        }
        return clones;
    }

    /**
     * Convert FROM employees ... => FROM *:employees,employees
     */
    static CsvSpecReader.CsvTestCase convertToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        if (dataLocation == null) {
            dataLocation = randomFrom(DataLocation.values());
        }
        if (testCase.requiredCapabilities.contains(SUBQUERY_IN_FROM_COMMAND.capabilityName())) {
            return convertSubqueryToRemoteIndices(testCase);
        }
        String query = testCase.query;
        // If true, we're using *:index, otherwise we're using *:index,index
        boolean onlyRemotes = canUseRemoteIndicesOnly() && randomBoolean();
        // Check if query contains enrich source indices - these are loaded into both clusters,
        // so we should use onlyRemotes=true to avoid duplicates
        var enrichSourceIndices = ENRICH_POLICIES.stream().map(CsvTestsDataLoader.EnrichConfig::index).collect(toSet());
        if (onlyRemotes == false && EsqlTestUtils.queryContainsIndices(query, enrichSourceIndices)) {
            onlyRemotes = true;
        }
        testCase.query = EsqlTestUtils.addRemoteIndices(testCase.query, LOOKUP_INDICES, onlyRemotes);

        int offset = testCase.query.length() - query.length();
        if (offset != 0) {
            final String pattern = "\\b1:(\\d+)\\b";
            final Pattern regex = Pattern.compile(pattern);
            testCase.adjustExpectedWarnings(warning -> regex.matcher(warning).replaceAll(match -> {
                int position = Integer.parseInt(match.group(1));
                int newPosition = position + offset;
                return "1:" + newPosition;
            }));
        }
        return testCase;
    }

    static boolean canUseRemoteIndicesOnly() {
        // If the data is indexed only into the remote cluster, we can query only the remote indices.
        // However, due to the union types bug in CCS, we must include the local indices in versions without the fix.
        return dataLocation == DataLocation.REMOTE_ONLY && Clusters.bwcVersion().onOrAfter(Version.V_9_1_0);
    }

    static boolean hasIndexMetadata(String query) {
        String[] commands = query.split("\\|");
        if (commands[0].trim().toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("(?i)metadata");
            return parts.length > 1 && parts[1].contains("_index");
        }
        return false;
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return Clusters.localClusterSupportsInferenceTestService();
    }

    @Override
    protected boolean supportsIndexModeLookup() throws IOException {
        return hasCapabilities(adminClient(), List.of(JOIN_LOOKUP_V12.capabilityName()));
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected boolean supportsTook() throws IOException {
        // We don't read took properly in multi-cluster tests.
        return false;
    }

    protected boolean supportsViews() {
        // MultiCluster CCS does not yet support VIEWS, due to rewriting FROM name to FROM *:name
        // In particular, we do not want to load views definitions, because that messes with `FROM *` queries
        // See, for example, "lookup-join/EnrichLookupStatsBug"
        return false;
    }

    @Override
    protected boolean clusterHasCapability(EsqlCapabilities.Cap capability) {
        try {
            return super.clusterHasCapability(capability) && hasCapabilities(remoteClusterClient(), List.of(capability.capabilityName()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert index patterns and subqueries in FROM commands to use remote indices for a given test case.
     */
    private static CsvSpecReader.CsvTestCase convertSubqueryToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        String query = testCase.query;
        testCase.query = EsqlTestUtils.convertSubqueryToRemoteIndices(query);
        return testCase;
    }
}
