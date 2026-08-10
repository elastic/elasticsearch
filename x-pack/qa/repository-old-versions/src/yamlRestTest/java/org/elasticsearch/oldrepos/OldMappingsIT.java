/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.oldelasticsearch.OldElasticsearchContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests old mappings restored from snapshots taken on pre-N-2 clusters.
 * <p>
 * Test methods are ordered so that {@link #testSurvivesRestart()} runs last: it performs a full cluster restart
 * and then re-verifies a subset of the restored data to ensure indices survive the restart.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@TestCaseOrdering(OldMappingsIT.RestartLastOrdering.class)
public class OldMappingsIT extends ESRestTestCase {

    /**
     * Orders test methods alphabetically but forces {@code testSurvivesRestart} to run last.
     */
    public static class RestartLastOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams a, TestMethodAndParams b) {
            boolean aIsRestart = a.getTestMethod().getName().equals("testSurvivesRestart");
            boolean bIsRestart = b.getTestMethod().getName().equals("testSurvivesRestart");
            if (aIsRestart != bIsRestart) {
                return aIsRestart ? 1 : -1;
            }
            return a.getTestMethod().getName().compareTo(b.getTestMethod().getName());
        }
    }

    private static final OldElasticsearchContainer oldEs = OldEsTestCluster.newContainer(OldMappingsIT.class);
    private static final ElasticsearchCluster cluster = OldEsTestCluster.newCluster(OldMappingsIT.class);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(oldEs).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

    static boolean setupDone;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            // increase the socket timeout so it doesn't fire before the increased ensureGreen timeout below;
            // waiting on cluster health after restoring/recovering the archived indices can take longer than
            // the default 30s/60s on busy/contended CI hosts (see ParameterizedRollingUpgradeTestCase).
            .put(CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @Override
    protected String getEnsureGreenTimeout() {
        // restoring a snapshot into a fresh cluster (and, after a restart, recovering it from disk) can take
        // longer than the default 30s on busy/contended CI hosts; align with the timeout used by other
        // BWC-style tests that wait on cluster health (see ParameterizedRollingUpgradeTestCase).
        return "70s";
    }

    @Before
    public void setupIndex() throws IOException {
        // The following is bit of a hack. While we wish we could make this an @BeforeClass, it does not work because the client() is only
        // initialized later, so we do it when running the first test
        if (setupDone) {
            return;
        }

        setupDone = true;

        String repoLocation = OldEsTestCluster.repoLocation(OldMappingsIT.class);

        String repoName = "old_mappings_repo";
        String snapshotName = "snap";
        List<String> indices = new ArrayList<>(List.of("filebeat", "custom", "nested", "standard_token_filter", "similarity"));
        if (oldVersion.before(Version.fromString("6.0.0"))) {
            indices.add("winlogbeat");
        }

        int oldEsPort = oldEs.getHttpPort();
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {

            assertOK(oldEs.performRequest(createIndex("filebeat", "filebeat.json")));
            if (oldVersion.before(Version.fromString("6.0.0"))) {
                assertOK(oldEs.performRequest(createIndex("winlogbeat", "winlogbeat.json")));
            }
            assertOK(
                oldEs.performRequest(
                    createIndex(
                        "standard_token_filter",
                        "standard_token_filter.json",
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.analysis.analyzer.custom_analyzer.type", "custom")
                            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
                            .put("index.analysis.analyzer.custom_analyzer.filter", "standard")
                            .build()
                    )
                )
            );
            assertOK(
                oldEs.performRequest(
                    createIndex(
                        "similarity",
                        "similarity.json",
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.similarity.custom_dfr.type", "DFR")
                            .put("index.similarity.custom_dfr.basic_model", randomFrom(Arrays.asList("be", "d", "p")))
                            .put("index.similarity.custom_dfr.after_effect", "no")
                            .put("index.similarity.custom_dfr.normalization", "h2")
                            .put("index.similarity.custom_dfr.normalization.h2.c", "3.0")
                            .build()
                    )
                )
            );
            assertOK(oldEs.performRequest(createIndex("custom", "custom.json")));
            assertOK(oldEs.performRequest(createIndex("nested", "nested.json")));

            Request doc1 = new Request("PUT", "/" + "custom" + "/" + "doc" + "/" + "1");
            doc1.addParameter("refresh", "true");
            XContentBuilder bodyDoc1 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl1")
                .field("agent", "agent1")
                .endObject()
                .endObject()
                .endObject();
            doc1.setJsonEntity(Strings.toString(bodyDoc1));
            assertOK(oldEs.performRequest(doc1));

            Request doc2 = new Request("PUT", "/" + "custom" + "/" + "doc" + "/" + "2");
            doc2.addParameter("refresh", "true");
            XContentBuilder bodyDoc2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl2")
                .field("agent", "agent2 agent2")
                .endObject()
                .endObject()
                .field("completion", "some_value")
                .endObject();
            doc2.setJsonEntity(Strings.toString(bodyDoc2));
            assertOK(oldEs.performRequest(doc2));

            Request doc3 = new Request("PUT", "/" + "nested" + "/" + "doc" + "/" + "1");
            doc3.addParameter("refresh", "true");
            XContentBuilder bodyDoc3 = XContentFactory.jsonBuilder()
                .startObject()
                .field("group", "fans")
                .startArray("user")
                .startObject()
                .field("first", "John")
                .field("last", "Smith")
                .endObject()
                .startObject()
                .field("first", "Alice")
                .field("last", "White")
                .endObject()
                .endArray()
                .endObject();
            doc3.setJsonEntity(Strings.toString(bodyDoc3));
            assertOK(oldEs.performRequest(doc3));

            Request doc4 = new Request("POST", "/" + "standard_token_filter" + "/" + "doc");
            doc4.addParameter("refresh", "true");
            XContentBuilder bodyDoc4 = XContentFactory.jsonBuilder().startObject().field("content", "Doc 1").endObject();
            doc4.setJsonEntity(Strings.toString(bodyDoc4));
            assertOK(oldEs.performRequest(doc4));

            Request doc5 = new Request("POST", "/" + "similarity" + "/" + "doc");
            doc5.addParameter("refresh", "true");
            XContentBuilder bodyDoc5 = XContentFactory.jsonBuilder().startObject().field("content", "Twin Peaks!").endObject();
            doc5.setJsonEntity(Strings.toString(bodyDoc5));
            assertOK(oldEs.performRequest(doc5));

            // register repo on old ES and take snapshot
            Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest.setJsonEntity(Strings.format("""
                {"type":"fs","settings":{"location":"%s"}}
                """, repoLocation));
            assertOK(oldEs.performRequest(createRepoRequest));

            Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
            assertOK(oldEs.performRequest(createSnapshotRequest));
        }
        // Snapshot is on disk — old ES is no longer needed. Stop it now to free memory during the remaining tests.
        oldEs.stop();

        // register repo on new ES and restore snapshot
        Request createRepoRequest2 = new Request("PUT", "/_snapshot/" + repoName);
        createRepoRequest2.setJsonEntity(Strings.format("""
            {"type":"fs","settings":{"location":"%s"}}
            """, repoLocation));
        assertOK(client().performRequest(createRepoRequest2));

        final Request createRestoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        createRestoreRequest.addParameter("wait_for_completion", "true");
        createRestoreRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
        createRestoreRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(createRestoreRequest));
    }

    private Request createIndex(String indexName, String file) throws IOException {
        return createIndex(indexName, file, Settings.EMPTY);
    }

    private Request createIndex(String indexName, String file, Settings settings) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        int numberOfShards = randomIntBetween(1, 3);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        builder.startObject("settings");
        builder.field(SETTING_NUMBER_OF_SHARDS, numberOfShards);
        settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        builder.startObject("mappings");
        builder.rawValue(OldMappingsIT.class.getResourceAsStream(file), XContentType.JSON);
        builder.endObject().endObject();

        createIndex.setJsonEntity(Strings.toString(builder));
        return createIndex;
    }

    public void testMappingOk() throws IOException {
        Request mappingRequest = new Request("GET", "/" + "filebeat" + "/_mapping");
        Map<String, Object> mapping = entityAsMap(client().performRequest(mappingRequest));
        assertNotNull(XContentMapValues.extractValue(mapping, "filebeat", "mappings", "properties", "apache2"));

        if (oldVersion.before(Version.fromString("6.0.0"))) {
            mappingRequest = new Request("GET", "/" + "winlogbeat" + "/_mapping");
            mapping = entityAsMap(client().performRequest(mappingRequest));
            assertNotNull(XContentMapValues.extractValue(mapping, "winlogbeat", "mappings", "properties", "message"));
        }
    }

    public void testStandardTokenFilter() throws IOException {
        assertMatchAll("standard_token_filter");
    }

    public void testSimilarityWithLegacySettings() throws IOException {
        assertMatchAll("similarity");
    }

    private void assertMatchAll(String indexName) throws IOException {
        Request search = new Request("POST", "/" + indexName + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match_all")
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
    }

    public void testSearchKeyword() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.url")
            .field("query", "myurl2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
    }

    public void testSearchOnPlaceHolderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("completion")
            .field("query", "some-agent")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(
            re.getMessage(),
            containsString("Field [completion] of type [completion] in legacy index does not support match queries")
        );
    }

    public void testAggregationOnPlaceholderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("aggs")
            .startObject("agents")
            .startObject("terms")
            .field("field", "completion")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(re.getMessage(), containsString("can't run aggregation or sorts on field type completion of legacy index"));
    }

    public void testConstantScoringOnTextField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.agent")
            .field("query", "agent2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> hit = (Map<String, Object>) hits.get(0);
        assertThat(hit, hasKey("_score"));
        assertEquals(1.0d, (double) hit.get("_score"), 0.01d);
    }

    public void testFieldsExistQueryOnTextField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("exists")
            .field("field", "apache2.access.agent")
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(2));
    }

    public void testSearchFieldsOnPlaceholderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.url")
            .field("query", "myurl2")
            .endObject()
            .endObject()
            .endObject()
            .startArray("fields")
            .value("completion")
            .endArray()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        logger.info(hits);
        Map<?, ?> fields = (Map<?, ?>) (XContentMapValues.extractValue("fields", (Map<?, ?>) hits.get(0)));
        assertEquals(List.of("some_value"), fields.get("completion"));
    }

    public void testNestedDocuments() throws IOException {
        Request search = new Request("POST", "/" + "nested" + "/_search");
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        logger.info(response);
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        Map<?, ?> source = (Map<?, ?>) (XContentMapValues.extractValue("_source", (Map<?, ?>) hits.get(0)));
        assertEquals("fans", source.get("group"));

        search = new Request("POST", "/" + "nested" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("nested")
            .field("path", "user")
            .startObject("query")
            .startObject("bool")
            .startArray("must")
            .startObject()
            .startObject("match")
            .field("user.first", "Alice")
            .endObject()
            .endObject()
            .startObject()
            .startObject("match")
            .field("user.last", "White")
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        response = entityAsMap(client().performRequest(search));
        logger.info(response);
        hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        source = (Map<?, ?>) (XContentMapValues.extractValue("_source", (Map<?, ?>) hits.get(0)));
        assertEquals("fans", source.get("group"));
    }

    /**
     * Runs last (via {@link RestartLastOrdering}). Performs a full cluster restart and verifies
     * that the restored indices are still accessible afterward.
     */
    public void testSurvivesRestart() throws IOException {
        // Verify data is accessible before restart
        testSearchKeyword();

        // Full cluster restart
        cluster.restart(false);
        // The restart allocates new HTTP ports; re-create the REST client so it points at the new addresses.
        closeClients();
        initClient();

        // Re-verify that restored indices are accessible after restart
        ensureGreen("filebeat");
        ensureGreen("custom");
        ensureGreen("nested");
        ensureGreen("standard_token_filter");
        ensureGreen("similarity");
        if (oldVersion.before(Version.fromString("6.0.0"))) {
            ensureGreen("winlogbeat");
        }

        // Re-run a key verification to confirm data survived the restart
        testMappingOk();
        testSearchKeyword();
    }
}
