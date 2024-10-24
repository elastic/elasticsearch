/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.search.SearchFeatures;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests to run before and after a full cluster restart. This is run twice,
 * one with {@code tests.is_old_cluster} set to {@code true} against a cluster
 * of an older version. The cluster is shutdown and a cluster of the new
 * version is started with the same data directories and then this is rerun
 * with {@code tests.is_old_cluster} set to {@code false}.
 */
public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static TemporaryFolder repoDirectory = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        // some tests rely on the translog not being flushed
        .setting("indices.memory.shard_inactive_time", "60m")
        .apply(() -> clusterConfig)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    private String index;

    public FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Before
    public void setIndex() {
        index = getRootTestName();
    }

    public void testSearch() throws Exception {
        int count;
        if (isRunningAgainstOldCluster()) {
            final var createIndex = newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();

                mappingsAndSettings.startObject("mappings");
                mappingsAndSettings.startObject("properties");
                {
                    mappingsAndSettings.startObject("string");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("dots_in_field_names");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("binary");
                    mappingsAndSettings.field("type", "binary");
                    mappingsAndSettings.field("store", "true");
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            });
            client().performRequest(createIndex);

            count = randomIntBetween(2000, 3000);
            byte[] randomByteArray = new byte[16];
            random().nextBytes(randomByteArray);
            indexRandomDocuments(
                count,
                true,
                true,
                randomBoolean(),
                i -> JsonXContent.contentBuilder()
                    .startObject()
                    .field("string", randomAlphaOfLength(10))
                    .field("int", randomInt(100))
                    .field("float", randomFloat())
                    // be sure to create a "proper" boolean (True, False) for the first document so that automapping is correct
                    .field("bool", i > 0 && randomBoolean())
                    .field("field.with.dots", randomAlphaOfLength(10))
                    .field("binary", Base64.getEncoder().encodeToString(randomByteArray))
                    .endObject()
            );
            refreshAllIndices();
        } else {
            count = countOfIndexedRandomDocuments();
        }

        ensureGreenLongWait(index);
        assertBasicSearchWorks(count);
        assertAllSearchWorks(count);
        assertBasicAggregationWorks();
        assertRealtimeGetWorks();
        assertStoredBinaryFields(count);
    }

    public void testNewReplicas() throws Exception {
        if (isRunningAgainstOldCluster()) {
            final var createIndex = newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();

                mappingsAndSettings.startObject("mappings");
                mappingsAndSettings.startObject("properties");
                {
                    mappingsAndSettings.startObject("field");
                    mappingsAndSettings.field("type", "text");
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            });
            client().performRequest(createIndex);

            int numDocs = randomIntBetween(2000, 3000);
            indexRandomDocuments(
                numDocs,
                true,
                false,
                randomBoolean(),
                i -> JsonXContent.contentBuilder().startObject().field("field", "value").endObject()
            );
            logger.info("Refreshing [{}]", index);
            client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        } else {
            // The test runs with two nodes so this should still go green.
            final int numReplicas = 1;
            final long startTime = System.currentTimeMillis();
            logger.debug("--> creating [{}] replicas for index [{}]", numReplicas, index);
            Request setNumberOfReplicas = newXContentRequest(
                HttpMethod.PUT,
                "/" + index + "/_settings",
                (builder, params) -> builder.startObject("index").field("number_of_replicas", numReplicas).endObject()
            );
            client().performRequest(setNumberOfReplicas);

            ensureGreenLongWait(index);

            logger.debug("--> index [{}] is green, took [{}] ms", index, (System.currentTimeMillis() - startTime));
            Map<String, Object> recoverRsp = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_recovery")));
            logger.debug("--> recovery status:\n{}", recoverRsp);

            Set<Integer> counts = new HashSet<>();
            for (String node : dataNodes(index, client())) {
                Request search = new Request("GET", "/" + index + "/_search");
                search.addParameter("preference", "_only_nodes:" + node);
                Map<String, Object> responseBody = entityAsMap(client().performRequest(search));
                assertNoFailures(responseBody);
                int hits = extractTotalHits(responseBody);
                counts.add(hits);
            }
            assertEquals("All nodes should have a consistent number of documents", 1, counts.size());
        }
    }

    public void testSearchTimeSeriesMode() throws Exception {
        assumeTrue("indexing time series indices changed in 8.2.0", oldClusterHasFeature(RestTestLegacyFeatures.TSDB_NEW_INDEX_FORMAT));
        int numDocs;
        if (isRunningAgainstOldCluster()) {
            numDocs = createTimeSeriesModeIndex(1);
        } else {
            numDocs = countOfIndexedRandomDocuments();
        }
        assertCountAll(numDocs);
        Request request = newXContentRequest(HttpMethod.GET, "/" + index + "/_search", (body, params) -> {
            body.field("size", 0);
            body.startObject("aggs").startObject("check").startObject("scripted_metric");
            body.field("init_script", "state.timeSeries = new HashSet()");
            body.field("map_script", "state.timeSeries.add(doc['dim'].value)");
            body.field("combine_script", "return state.timeSeries");
            body.field("reduce_script", """
                Set timeSeries = new TreeSet();
                for (s in states) {
                  for (ts in s) {
                    boolean newTs = timeSeries.add(ts);
                    if (false == newTs) {
                      throw new IllegalArgumentException(ts + ' appeared in two shards');
                    }
                  }
                }
                return timeSeries;""");
            body.endObject().endObject().endObject();
            return body;
        });
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertMap(
            response,
            matchesMap().extraOk()
                .entry("hits", matchesMap().extraOk().entry("total", Map.of("value", numDocs, "relation", "eq")))
                .entry("aggregations", Map.of("check", Map.of("value", IntStream.range(0, 10).mapToObj(i -> "dim" + i).collect(toList()))))
        );
    }

    public void testNewReplicasTimeSeriesMode() throws Exception {
        assumeTrue("indexing time series indices changed in 8.2.0", oldClusterHasFeature(RestTestLegacyFeatures.TSDB_NEW_INDEX_FORMAT));
        if (isRunningAgainstOldCluster()) {
            createTimeSeriesModeIndex(0);
        } else {
            // The test runs with two nodes so this should still go green.
            final int numReplicas = 1;
            final long startTime = System.currentTimeMillis();
            logger.debug("--> creating [{}] replicas for index [{}]", numReplicas, index);
            Request setNumberOfReplicas = newXContentRequest(
                HttpMethod.PUT,
                "/" + index + "/_settings",
                (builder, params) -> builder.startObject("index").field("number_of_replicas", numReplicas).endObject()
            );
            client().performRequest(setNumberOfReplicas);

            ensureGreenLongWait(index);

            logger.debug("--> index [{}] is green, took [{}] ms", index, (System.currentTimeMillis() - startTime));
            Map<String, Object> recoverRsp = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_recovery")));
            logger.debug("--> recovery status:\n{}", recoverRsp);

            Set<Integer> counts = new HashSet<>();
            for (String node : dataNodes(index, client())) {
                Request search = new Request("GET", "/" + index + "/_search");
                search.addParameter("preference", "_only_nodes:" + node);
                Map<String, Object> responseBody = entityAsMap(client().performRequest(search));
                assertNoFailures(responseBody);
                int hits = extractTotalHits(responseBody);
                counts.add(hits);
            }
            assertEquals("All nodes should have a consistent number of documents", 1, counts.size());
        }
    }

    private int createTimeSeriesModeIndex(int replicas) throws IOException {
        final var createIndex = newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
            mappingsAndSettings.startObject("settings");
            mappingsAndSettings.field("number_of_shards", 1);
            mappingsAndSettings.field("number_of_replicas", replicas);
            mappingsAndSettings.field("mode", "time_series");
            mappingsAndSettings.field("routing_path", "dim");
            mappingsAndSettings.field("time_series.start_time", 1L);
            mappingsAndSettings.field("time_series.end_time", DateUtils.MAX_MILLIS_BEFORE_9999 - 1);
            mappingsAndSettings.endObject();

            mappingsAndSettings.startObject("mappings");
            mappingsAndSettings.startObject("properties");
            {
                mappingsAndSettings.startObject("@timestamp").field("type", "date").endObject();
                mappingsAndSettings.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            }
            mappingsAndSettings.endObject();
            mappingsAndSettings.endObject();
            return mappingsAndSettings;
        });
        client().performRequest(createIndex);

        int numDocs = randomIntBetween(2000, 3000);
        long basetime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2021-01-01T00:00:00Z");
        indexRandomDocuments(
            numDocs,
            true,
            true,
            false,
            i -> JsonXContent.contentBuilder()
                .startObject()
                .field("@timestamp", basetime + TimeUnit.MINUTES.toMillis(i))
                .field("dim", "dim" + (i % 10))
                .endObject()
        );
        logger.info("Refreshing [{}]", index);
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        return numDocs;
    }

    public void testClusterState() throws Exception {
        if (isRunningAgainstOldCluster()) {
            final Request createTemplate = newXContentRequest(HttpMethod.PUT, "/_template/template_1", (mappingsAndSettings, params) -> {
                mappingsAndSettings.field("index_patterns", index);
                mappingsAndSettings.field("order", "1000");
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            });
            createTemplate.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
            client().performRequest(createTemplate);
            client().performRequest(new Request("PUT", "/" + index));
        }

        // verifying if we can still read some properties from cluster state api:
        Map<String, Object> clusterState = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));

        // Check some global properties:
        String numberOfShards = (String) XContentMapValues.extractValue(
            "metadata.templates.template_1.settings.index.number_of_shards",
            clusterState
        );
        assertEquals("1", numberOfShards);
        String numberOfReplicas = (String) XContentMapValues.extractValue(
            "metadata.templates.template_1.settings.index.number_of_replicas",
            clusterState
        );
        assertEquals("0", numberOfReplicas);

        // Check some index properties:
        numberOfShards = (String) XContentMapValues.extractValue(
            "metadata.indices." + index + ".settings.index.number_of_shards",
            clusterState
        );
        assertEquals("1", numberOfShards);
        numberOfReplicas = (String) XContentMapValues.extractValue(
            "metadata.indices." + index + ".settings.index.number_of_replicas",
            clusterState
        );
        assertEquals("0", numberOfReplicas);
        IndexVersion version = IndexVersion.fromId(
            Integer.valueOf(
                (String) XContentMapValues.extractValue("metadata.indices." + index + ".settings.index.version.created", clusterState)
            )
        );
        assertEquals(getOldClusterIndexVersion(), version);

    }

    public void testShrink() throws IOException {
        String shrunkenIndex = index + "_shrunk";
        int numDocs;
        if (isRunningAgainstOldCluster()) {
            final var createIndex = newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("mappings");
                {
                    mappingsAndSettings.startObject("properties");
                    {
                        mappingsAndSettings.startObject("field");
                        {
                            mappingsAndSettings.field("type", "text");
                        }
                        mappingsAndSettings.endObject();
                    }
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();

                mappingsAndSettings.startObject("settings");
                {
                    mappingsAndSettings.field("index.number_of_shards", 5);
                }
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            });
            client().performRequest(createIndex);

            numDocs = randomIntBetween(512, 1024);
            indexRandomDocuments(
                numDocs,
                true,
                true,
                randomBoolean(),
                i -> JsonXContent.contentBuilder().startObject().field("field", "value").endObject()
            );

            ensureGreen(index); // wait for source index to be available on both nodes before starting shrink

            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/" + index + "/_settings",
                    (builder, params) -> builder.startObject("settings").field("index.blocks.write", true).endObject()
                )
            );

            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/" + index + "/_shrink/" + shrunkenIndex,
                    (builder, params) -> builder.startObject("settings").field("index.number_of_shards", 1).endObject()
                )
            );

            refreshAllIndices();
        } else {
            numDocs = countOfIndexedRandomDocuments();
        }

        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")));
        assertNoFailures(response);
        int totalShards = (int) XContentMapValues.extractValue("_shards.total", response);
        assertThat(totalShards, greaterThan(1));
        int successfulShards = (int) XContentMapValues.extractValue("_shards.successful", response);
        assertEquals(totalShards, successfulShards);
        int totalHits = extractTotalHits(response);
        assertEquals(numDocs, totalHits);

        response = entityAsMap(client().performRequest(new Request("GET", "/" + shrunkenIndex + "/_search")));
        assertNoFailures(response);
        totalShards = (int) XContentMapValues.extractValue("_shards.total", response);
        assertEquals(1, totalShards);
        successfulShards = (int) XContentMapValues.extractValue("_shards.successful", response);
        assertEquals(1, successfulShards);
        totalHits = extractTotalHits(response);
        assertEquals(numDocs, totalHits);
    }

    public void testShrinkAfterUpgrade() throws IOException {
        String shrunkenIndex = index + "_shrunk";
        int numDocs;
        if (isRunningAgainstOldCluster()) {
            final var createIndex = newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("mappings");
                {
                    mappingsAndSettings.startObject("properties");
                    {
                        mappingsAndSettings.startObject("field");
                        {
                            mappingsAndSettings.field("type", "text");
                        }
                        mappingsAndSettings.endObject();
                    }
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                // the default number of shards is now one so we have to set the number of shards to be more than one explicitly
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("index.number_of_shards", 5);
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            });
            client().performRequest(createIndex);

            numDocs = randomIntBetween(512, 1024);
            indexRandomDocuments(
                numDocs,
                true,
                true,
                randomBoolean(),
                i -> JsonXContent.contentBuilder().startObject().field("field", "value").endObject()
            );
        } else {
            ensureGreen(index); // wait for source index to be available on both nodes before starting shrink

            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/" + index + "/_settings",
                    (builder, params) -> builder.startObject("settings").field("index.blocks.write", true).endObject()
                )
            );

            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/" + index + "/_shrink/" + shrunkenIndex,
                    (builder, params) -> builder.startObject("settings").field("index.number_of_shards", 1).endObject()
                )
            );

            numDocs = countOfIndexedRandomDocuments();
        }

        refreshAllIndices();

        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")));
        assertNoFailures(response);
        int totalShards = (int) XContentMapValues.extractValue("_shards.total", response);
        assertThat(totalShards, greaterThan(1));
        int successfulShards = (int) XContentMapValues.extractValue("_shards.successful", response);
        assertEquals(totalShards, successfulShards);
        int totalHits = extractTotalHits(response);
        assertEquals(numDocs, totalHits);

        if (isRunningAgainstOldCluster() == false) {
            response = entityAsMap(client().performRequest(new Request("GET", "/" + shrunkenIndex + "/_search")));
            assertNoFailures(response);
            totalShards = (int) XContentMapValues.extractValue("_shards.total", response);
            assertEquals(1, totalShards);
            successfulShards = (int) XContentMapValues.extractValue("_shards.successful", response);
            assertEquals(1, successfulShards);
            totalHits = extractTotalHits(response);
            assertEquals(numDocs, totalHits);
        }
    }

    /**
     * Test upgrading after a rollover. Specifically:
     * <ol>
     *  <li>Create an index with a write alias
     *  <li>Write some documents to the write alias
     *  <li>Roll over the index
     *  <li>Make sure the document count is correct
     *  <li>Upgrade
     *  <li>Write some more documents to the write alias
     *  <li>Make sure the document count is correct
     * </ol>
     */
    public void testRollover() throws Exception {
        if (isRunningAgainstOldCluster()) {
            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/" + index + "-000001",
                    (builder, params) -> builder.startObject("aliases").startObject(index + "_write").endObject().endObject()
                )
            );
        }

        int bulkCount = 10;
        String bulk = """
            {"index":{}}
            {"test":"test"}
            """.repeat(bulkCount);

        Request bulkRequest = new Request("POST", "/" + index + "_write/_bulk");

        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "");
        assertThat(EntityUtils.toString(client().performRequest(bulkRequest).getEntity()), containsString("\"errors\":false"));

        if (isRunningAgainstOldCluster()) {
            client().performRequest(
                newXContentRequest(
                    HttpMethod.POST,
                    "/" + index + "_write/_rollover",
                    (builder, params) -> builder.startObject("conditions").field("max_docs", 5).endObject()
                )
            );

            // assertBusy to work around https://github.com/elastic/elasticsearch/issues/104371
            assertBusy(
                () -> assertThat(
                    EntityUtils.toString(client().performRequest(new Request("GET", "/_cat/indices?v&error_trace")).getEntity()),
                    containsString("testrollover-000002")
                )
            );
        }

        Request countRequest = new Request("POST", "/" + index + "-*/_search");
        countRequest.addParameter("size", "0");
        Map<String, Object> count = entityAsMap(client().performRequest(countRequest));
        assertNoFailures(count);

        int expectedCount = bulkCount + (isRunningAgainstOldCluster() ? 0 : bulkCount);
        assertEquals(expectedCount, extractTotalHits(count));
    }

    void assertCountAll(int count) throws IOException {
        Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")));
        assertNoFailures(response);
        int numDocs = extractTotalHits(response);
        logger.info("Found {} in old index", numDocs);
        assertEquals(count, numDocs);
    }

    void assertBasicSearchWorks(int count) throws IOException {
        logger.info("--> testing basic search");
        {
            assertCountAll(count);
        }

        logger.info("--> testing basic search with sort");
        {
            Map<String, Object> response = entityAsMap(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.GET,
                        "/" + index + "/_search",
                        (builder, params) -> builder.startArray("sort").startObject().field("int", "asc").endObject().endArray()
                    )
                )
            );
            assertNoFailures(response);
            assertTotalHits(count, response);
        }

        logger.info("--> testing exists filter");
        {
            Map<String, Object> response = entityAsMap(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.GET,
                        "/" + index + "/_search",
                        (builder, params) -> builder.startObject("query")
                            .startObject("exists")
                            .field("field", "string")
                            .endObject()
                            .endObject()
                    )
                )
            );
            assertNoFailures(response);
            assertTotalHits(count, response);
        }

        logger.info("--> testing field with dots in the name");
        {
            Map<String, Object> response = entityAsMap(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.GET,
                        "/" + index + "/_search",
                        (builder, params) -> builder.startObject("query")
                            .startObject("exists")
                            .field("field", "field.with.dots")
                            .endObject()
                            .endObject()
                    )
                )
            );
            assertNoFailures(response);
            assertTotalHits(count, response);
        }
    }

    void assertAllSearchWorks(int count) throws IOException {
        logger.info("--> testing _all search");
        Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")));
        assertNoFailures(response);
        assertTotalHits(count, response);
        Map<?, ?> bestHit = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("hits.hits", response))).get(0);

        // Make sure there are payloads and they are taken into account for the score
        // the 'string' field has a boost of 4 in the mappings so it should get a payload boost
        String stringValue = (String) XContentMapValues.extractValue("_source.string", bestHit);
        assertNotNull(stringValue);
        String id = (String) bestHit.get("_id");

        String explanation = toStr(
            client().performRequest(
                newXContentRequest(
                    HttpMethod.GET,
                    "/" + index + "/_explain/" + id,
                    (builder, params) -> builder.startObject("query").startObject("match_all").endObject().endObject()
                )
            )
        );
        assertFalse("Could not find payload boost in explanation\n" + explanation, explanation.contains("payloadBoost"));

        // Make sure the query can run on the whole index
        Request searchRequest = newXContentRequest(
            HttpMethod.GET,
            "/" + index + "/_search",
            (builder, params) -> builder.startObject("query").startObject("match_all").endObject().endObject()
        );
        searchRequest.addParameter("explain", "true");
        Map<?, ?> matchAllResponse = entityAsMap(client().performRequest(searchRequest));
        assertNoFailures(matchAllResponse);
        assertTotalHits(count, matchAllResponse);
    }

    void assertBasicAggregationWorks() throws IOException {
        // histogram on a long
        Map<?, ?> longHistogram = entityAsMap(
            client().performRequest(
                newXContentRequest(
                    HttpMethod.GET,
                    "/" + index + "/_search",
                    (builder, params) -> builder.startObject("aggs")
                        .startObject("histo")
                        .startObject("histogram")
                        .field("field", "int")
                        .field("interval", 10)
                        .endObject()
                        .endObject()
                        .endObject()
                )
            )
        );
        assertNoFailures(longHistogram);
        List<?> histoBuckets = (List<?>) XContentMapValues.extractValue("aggregations.histo.buckets", longHistogram);
        int histoCount = 0;
        for (Object entry : histoBuckets) {
            Map<?, ?> bucket = (Map<?, ?>) entry;
            histoCount += (Integer) bucket.get("doc_count");
        }
        assertTotalHits(histoCount, longHistogram);

        // terms on a boolean
        Map<?, ?> boolTerms = entityAsMap(
            client().performRequest(
                newXContentRequest(
                    HttpMethod.GET,
                    "/" + index + "/_search",
                    (builder, params) -> builder.startObject("aggs")
                        .startObject("bool_terms")
                        .startObject("terms")
                        .field("field", "bool")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            )
        );
        List<?> termsBuckets = (List<?>) XContentMapValues.extractValue("aggregations.bool_terms.buckets", boolTerms);
        int termsCount = 0;
        for (Object entry : termsBuckets) {
            Map<?, ?> bucket = (Map<?, ?>) entry;
            termsCount += (Integer) bucket.get("doc_count");
        }
        assertTotalHits(termsCount, boolTerms);
    }

    void assertRealtimeGetWorks() throws IOException {
        client().performRequest(
            newXContentRequest(
                HttpMethod.PUT,
                "/" + index + "/_settings",
                (builder, params) -> builder.startObject("index").field("refresh_interval", -1).endObject()
            )
        );

        Map<?, ?> searchResponse = entityAsMap(
            client().performRequest(
                newXContentRequest(
                    HttpMethod.GET,
                    "/" + index + "/_search",
                    (builder, params) -> builder.startObject("query").startObject("match_all").endObject().endObject()
                )
            )
        );
        Map<?, ?> hit = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("hits.hits", searchResponse))).get(0);
        String docId = (String) hit.get("_id");

        client().performRequest(
            newXContentRequest(
                HttpMethod.POST,
                "/" + index + "/_update/" + docId,
                (builder, params) -> builder.startObject("doc").field("foo", "bar").endObject()
            )
        );

        Request getRequest = new Request("GET", "/" + index + "/_doc/" + docId);

        Map<String, Object> getRsp = entityAsMap(client().performRequest(getRequest));
        Map<?, ?> source = (Map<?, ?>) getRsp.get("_source");
        assertTrue("doc does not contain 'foo' key: " + source, source.containsKey("foo"));

        client().performRequest(
            newXContentRequest(
                HttpMethod.PUT,
                "/" + index + "/_settings",
                (builder, params) -> builder.startObject("index").field("refresh_interval", "1s").endObject()
            )
        );
    }

    void assertStoredBinaryFields(int count) throws Exception {
        final var restResponse = client().performRequest(
            newXContentRequest(
                HttpMethod.GET,
                "/" + index + "/_search",
                (builder, params) -> builder.startObject("query")
                    .startObject("match_all")
                    .endObject()
                    .endObject()
                    .field("size", 100)
                    .field("stored_fields", "binary")
            )
        );
        Map<String, Object> rsp = entityAsMap(restResponse);

        assertTotalHits(count, rsp);
        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", rsp);
        assertEquals(100, hits.size());
        for (Object hit : hits) {
            Map<?, ?> hitRsp = (Map<?, ?>) hit;
            List<?> values = (List<?>) XContentMapValues.extractValue("fields.binary", hitRsp);
            assertEquals(1, values.size());
            byte[] binaryValue = switch (XContentType.fromMediaType(restResponse.getEntity().getContentType().getValue())) {
                case JSON, VND_JSON -> Base64.getDecoder().decode((String) values.get(0));
                case SMILE, CBOR, YAML, VND_SMILE, VND_CBOR, VND_YAML -> (byte[]) values.get(0);
            };
            assertEquals("Unexpected binary length [" + Base64.getEncoder().encodeToString(binaryValue) + "]", 16, binaryValue.length);
        }
    }

    static String toStr(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }

    static void assertNoFailures(Map<?, ?> response) {
        int failed = (int) XContentMapValues.extractValue("_shards.failed", response);
        assertEquals(0, failed);
    }

    void assertTotalHits(int expectedTotalHits, Map<?, ?> response) {
        int actualTotalHits = extractTotalHits(response);
        assertEquals(response.toString(), expectedTotalHits, actualTotalHits);
    }

    static int extractTotalHits(Map<?, ?> response) {
        return (Integer) XContentMapValues.extractValue("hits.total.value", response);
    }

    /**
     * Tests that a single document survives. Super basic smoke test.
     */
    public void testSingleDoc() throws IOException {
        String docLocation = "/" + index + "/_doc/1";
        String doc = "{\"test\": \"test\"}";

        if (isRunningAgainstOldCluster()) {
            Request createDoc = new Request("PUT", docLocation);
            createDoc.setJsonEntity(doc);
            client().performRequest(createDoc);
        }

        Request request = new Request("GET", docLocation);
        assertThat(toStr(client().performRequest(request)), containsString(doc));
    }

    /**
     * Tests that a single empty shard index is correctly recovered. Empty shards are often an edge case.
     */
    public void testEmptyShard() throws IOException {
        final String indexName = "test_empty_shard";

        if (isRunningAgainstOldCluster()) {
            Settings.Builder settings = indexSettings(1, 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            createIndex(indexName, settings.build());
        }
        ensureGreen(indexName);
    }

    /**
     * Tests recovery of an index.
     */
    public void testRecovery() throws Exception {
        int count;
        if (isRunningAgainstOldCluster()) {
            count = between(200, 300);
            Settings.Builder settings = Settings.builder();
            if (minimumIndexVersion().before(IndexVersions.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            final String mappings = randomBoolean() ? "\"_source\": { \"enabled\": false}" : null;
            createIndex(index, settings.build(), mappings);
            indexRandomDocuments(count, true, true, true, i -> jsonBuilder().startObject().field("field", "value").endObject());

            // make sure all recoveries are done
            ensureGreen(index);

            // Force flush so we're sure that all translog are committed
            Request flushRequest = new Request("POST", "/" + index + "/_flush");
            flushRequest.addParameter("force", "true");
            flushRequest.addParameter("wait_if_ongoing", "true");
            assertOK(client().performRequest(flushRequest));
        } else {
            count = countOfIndexedRandomDocuments();
        }

        // Count the documents in the index to make sure we have as many as we put there
        Request countRequest = new Request("GET", "/" + index + "/_search");
        countRequest.addParameter("size", "0");
        refreshAllIndices();
        Map<String, Object> countResponse = entityAsMap(client().performRequest(countRequest));
        assertTotalHits(count, countResponse);

        if (false == isRunningAgainstOldCluster()) {
            boolean foundPrimary = false;
            Request recoveryRequest = new Request("GET", "/_cat/recovery/" + index);
            recoveryRequest.addParameter("h", "index,shard,type,stage,translog_ops_recovered");
            recoveryRequest.addParameter("s", "index,shard,type");
            String recoveryResponse = toStr(client().performRequest(recoveryRequest));
            foundPrimary = recoveryResponse.split("\n").length > 0;
            assertTrue("expected to find a primary but didn't\n" + recoveryResponse, foundPrimary);
        }
    }

    /**
     * Tests snapshot/restore by creating a snapshot and restoring it. It takes
     * a snapshot on the old cluster and restores it on the old cluster as a
     * sanity check and on the new cluster as an upgrade test. It also takes a
     * snapshot on the new cluster and restores that on the new cluster as a
     * test that the repository is ok with containing snapshot from both the
     * old and new versions. All of the snapshots include an index, a template,
     * and some routing configuration.
     */
    public void testSnapshotRestore() throws IOException {
        int count;
        if (isRunningAgainstOldCluster()) {
            // Create the index
            count = between(200, 300);
            Settings.Builder settings = Settings.builder();
            if (minimumIndexVersion().before(IndexVersions.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            createIndex(index, settings.build());
            indexRandomDocuments(count, true, true, randomBoolean(), i -> jsonBuilder().startObject().field("field", "value").endObject());
        } else {
            count = countOfIndexedRandomDocuments();
        }

        // Refresh the index so the count doesn't fail
        refreshAllIndices();

        // Count the documents in the index to make sure we have as many as we put there
        Request countRequest = new Request("GET", "/" + index + "/_search");
        countRequest.addParameter("size", "0");
        Map<String, Object> countResponse = entityAsMap(client().performRequest(countRequest));
        assertTotalHits(count, countResponse);

        // Stick a routing attribute into to cluster settings so we can see it after the restore
        client().performRequest(
            newXContentRequest(
                HttpMethod.PUT,
                "/_cluster/settings",
                (builder, params) -> builder.startObject("persistent")
                    .field("cluster.routing.allocation.exclude.test_attr", getOldClusterVersion())
                    .endObject()
            )
        );

        // Stick a template into the cluster so we can see it after the restore
        Request createTemplateRequest = newXContentRequest(HttpMethod.PUT, "/_template/test_template", (templateBuilder, params) -> {
            templateBuilder.field("index_patterns", "evil_*"); // Don't confuse other tests by applying the template
            templateBuilder.startObject("settings");
            {
                templateBuilder.field("number_of_shards", 1);
            }
            templateBuilder.endObject();
            templateBuilder.startObject("mappings");
            {
                {
                    templateBuilder.startObject("_source");
                    {
                        templateBuilder.field("enabled", true);
                    }
                    templateBuilder.endObject();
                }
            }
            templateBuilder.endObject();
            templateBuilder.startObject("aliases");
            {
                templateBuilder.startObject("alias1").endObject();
                templateBuilder.startObject("alias2");
                {
                    templateBuilder.startObject("filter");
                    {
                        templateBuilder.startObject("term");
                        {
                            templateBuilder.field(
                                "version",
                                isRunningAgainstOldCluster() ? getOldClusterVersion() : Build.current().version()
                            );
                        }
                        templateBuilder.endObject();
                    }
                    templateBuilder.endObject();
                }
                templateBuilder.endObject();
            }
            templateBuilder.endObject();
            return templateBuilder;
        });
        createTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createTemplateRequest);

        if (isRunningAgainstOldCluster()) {
            // Create the repo
            client().performRequest(newXContentRequest(HttpMethod.PUT, "/_snapshot/repo", (repoConfig, params) -> {
                repoConfig.field("type", "fs");
                repoConfig.startObject("settings");
                {
                    repoConfig.field("compress", randomBoolean());
                    repoConfig.field("location", repoDirectory.getRoot().getPath());
                }
                return repoConfig.endObject();
            }));
        }

        Request createSnapshot = newXContentRequest(
            HttpMethod.PUT,
            "/_snapshot/repo/" + (isRunningAgainstOldCluster() ? "old_snap" : "new_snap"),
            (builder, params) -> builder.field("indices", index)
        );
        createSnapshot.addParameter("wait_for_completion", "true");
        client().performRequest(createSnapshot);

        checkSnapshot("old_snap", count, getOldClusterVersion(), getOldClusterIndexVersion());
        if (false == isRunningAgainstOldCluster()) {
            checkSnapshot("new_snap", count, Build.current().version(), IndexVersion.current());
        }
    }

    public void testHistoryUUIDIsAdded() throws Exception {
        if (isRunningAgainstOldCluster()) {
            client().performRequest(newXContentRequest(HttpMethod.PUT, '/' + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 1);
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            }));
        } else {
            ensureGreenLongWait(index);

            Request statsRequest = new Request("GET", index + "/_stats");
            statsRequest.addParameter("level", "shards");
            Response response = client().performRequest(statsRequest);
            List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards.0");
            assertThat(shardStats, notNullValue());
            assertThat("Expected stats for 2 shards", shardStats, hasSize(2));
            String globalHistoryUUID = null;
            for (Object shard : shardStats) {
                final String nodeId = ObjectPath.evaluate(shard, "routing.node");
                final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
                logger.info("evaluating: {} , {}", ObjectPath.evaluate(shard, "routing"), ObjectPath.evaluate(shard, "commit"));
                String historyUUID = ObjectPath.evaluate(shard, "commit.user_data.history_uuid");
                assertThat("no history uuid found on " + nodeId + " (primary: " + primary + ")", historyUUID, notNullValue());
                if (globalHistoryUUID == null) {
                    globalHistoryUUID = historyUUID;
                } else {
                    assertThat(
                        "history uuid mismatch on " + nodeId + " (primary: " + primary + ")",
                        historyUUID,
                        equalTo(globalHistoryUUID)
                    );
                }
            }
        }
    }

    public void testSoftDeletes() throws Exception {
        if (isRunningAgainstOldCluster()) {
            client().performRequest(newXContentRequest(HttpMethod.PUT, "/" + index, (mappingsAndSettings, params) -> {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 1);
                if (randomBoolean()) {
                    mappingsAndSettings.field("soft_deletes.enabled", true);
                }
                mappingsAndSettings.endObject();
                return mappingsAndSettings;
            }));
            int numDocs = between(10, 100);
            for (int i = 0; i < numDocs; i++) {
                client().performRequest(
                    newXContentRequest(HttpMethod.POST, "/" + index + "/_doc/" + i, (builder, params) -> builder.field("field", "v1"))
                );
                refreshAllIndices();
            }
            client().performRequest(new Request("POST", "/" + index + "/_flush"));
            int liveDocs = numDocs;
            assertTotalHits(liveDocs, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
            for (int i = 0; i < numDocs; i++) {
                if (randomBoolean()) {
                    client().performRequest(
                        newXContentRequest(HttpMethod.POST, "/" + index + "/_doc/" + i, (builder, params) -> builder.field("field", "v2"))
                    );
                } else if (randomBoolean()) {
                    client().performRequest(new Request("DELETE", "/" + index + "/_doc/" + i));
                    liveDocs--;
                }
            }
            refreshAllIndices();
            assertTotalHits(liveDocs, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
            saveInfoDocument(index + "_doc_count", Integer.toString(liveDocs));
        } else {
            int liveDocs = Integer.parseInt(loadInfoDocument(index + "_doc_count"));
            assertTotalHits(liveDocs, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
        }
    }

    /**
     * This test creates an index in the old cluster and then closes it. When the cluster is fully restarted in a newer version,
     * it verifies that the index exists and is replicated if the old version supports replication.
     */
    public void testClosedIndices() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createIndex(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
            ensureGreen(index);

            int numDocs = 0;
            if (randomBoolean()) {
                numDocs = between(1, 100);
                for (int i = 0; i < numDocs; i++) {
                    assertOK(
                        client().performRequest(
                            newXContentRequest(
                                HttpMethod.POST,
                                "/" + index + "/_doc/" + i,
                                (builder, params) -> builder.field("field", "v1")
                            )
                        )
                    );
                    if (rarely()) {
                        refreshAllIndices();
                    }
                }
                refreshAllIndices();
            }

            assertTotalHits(numDocs, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
            saveInfoDocument(index + "_doc_count", Integer.toString(numDocs));
            closeIndex(index);
        }

        ensureGreenLongWait(index);
        assertClosedIndex(index, true);

        if (isRunningAgainstOldCluster() == false) {
            openIndex(index);
            ensureGreen(index);

            final int expectedNumDocs = Integer.parseInt(loadInfoDocument(index + "_doc_count"));
            assertTotalHits(expectedNumDocs, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
        }
    }

    /**
     * Asserts that an index is closed in the cluster state. If `checkRoutingTable` is true, it also asserts
     * that the index has started shards.
     */
    @SuppressWarnings("unchecked")
    private void assertClosedIndex(final String indexName, final boolean checkRoutingTable) throws IOException {
        final Map<String, ?> state = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));

        final Map<String, ?> metadata = (Map<String, Object>) XContentMapValues.extractValue("metadata.indices." + indexName, state);
        assertThat(metadata, notNullValue());
        assertThat(metadata.get("state"), equalTo("close"));

        final Map<String, ?> blocks = (Map<String, Object>) XContentMapValues.extractValue("blocks.indices." + indexName, state);
        assertThat(blocks, notNullValue());
        assertThat(blocks.containsKey(String.valueOf(MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)), is(true));

        final Map<String, ?> settings = (Map<String, Object>) XContentMapValues.extractValue("settings", metadata);
        assertThat(settings, notNullValue());

        final Map<String, ?> routingTable = (Map<String, Object>) XContentMapValues.extractValue(
            "routing_table.indices." + indexName,
            state
        );
        if (checkRoutingTable) {
            assertThat(routingTable, notNullValue());
            assertThat(Booleans.parseBoolean((String) XContentMapValues.extractValue("index.verified_before_close", settings)), is(true));
            final String numberOfShards = (String) XContentMapValues.extractValue("index.number_of_shards", settings);
            assertThat(numberOfShards, notNullValue());
            final int nbShards = Integer.parseInt(numberOfShards);
            assertThat(nbShards, greaterThanOrEqualTo(1));

            for (int i = 0; i < nbShards; i++) {
                final Collection<Map<String, ?>> shards = (Collection<Map<String, ?>>) XContentMapValues.extractValue(
                    "shards." + i,
                    routingTable
                );
                assertThat(shards, notNullValue());
                assertThat(shards.size(), equalTo(2));
                for (Map<String, ?> shard : shards) {
                    assertThat(XContentMapValues.extractValue("shard", shard), equalTo(i));
                    assertThat(XContentMapValues.extractValue("state", shard), equalTo("STARTED"));
                    assertThat(XContentMapValues.extractValue("index", shard), equalTo(indexName));
                }
            }
        } else {
            assertThat(routingTable, nullValue());
            assertThat(XContentMapValues.extractValue("index.verified_before_close", settings), nullValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void checkSnapshot(String snapshotName, int count, String tookOnVersion, IndexVersion tookOnIndexVersion) throws IOException {
        // Check the snapshot metadata, especially the version
        Request listSnapshotRequest = new Request("GET", "/_snapshot/repo/" + snapshotName);
        Map<String, Object> snapResponse = entityAsMap(client().performRequest(listSnapshotRequest));

        assertEquals(singletonList(snapshotName), XContentMapValues.extractValue("snapshots.snapshot", snapResponse));
        assertEquals(singletonList("SUCCESS"), XContentMapValues.extractValue("snapshots.state", snapResponse));
        // the format can change depending on the ES node version running & this test code running
        assertThat(
            XContentMapValues.extractValue("snapshots.version", snapResponse),
            anyOf(
                equalTo(List.of(tookOnVersion)),
                equalTo(List.of(tookOnIndexVersion.toString())),
                equalTo(List.of(tookOnIndexVersion.toReleaseVersion()))
            )
        );

        // Remove the routing setting and template so we can test restoring them.
        client().performRequest(
            newXContentRequest(
                HttpMethod.PUT,
                "/_cluster/settings",
                (builder, params) -> builder.startObject("persistent").nullField("cluster.routing.allocation.exclude.test_attr").endObject()
            )
        );

        client().performRequest(new Request("DELETE", "/_template/test_template"));

        // Restore
        Request restoreRequest = newXContentRequest(
            HttpMethod.POST,
            "/_snapshot/repo/" + snapshotName + "/_restore",
            (restoreCommand, params) -> {
                restoreCommand.field("include_global_state", true);
                restoreCommand.field("indices", index);
                restoreCommand.field("rename_pattern", index);
                restoreCommand.field("rename_replacement", "restored_" + index);
                return restoreCommand;
            }
        );
        restoreRequest.addParameter("wait_for_completion", "true");
        client().performRequest(restoreRequest);

        // Make sure search finds all documents
        Request countRequest = new Request("GET", "/restored_" + index + "/_search");
        countRequest.addParameter("size", "0");
        Map<String, Object> countResponse = entityAsMap(client().performRequest(countRequest));
        assertTotalHits(count, countResponse);

        // Add some extra documents to the index to be sure we can still write to it after restoring it
        int extras = between(1, 100);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < extras; i++) {
            bulk.append(Strings.format("""
                {"index":{"_id":"%s"}}
                {"test":"test"}
                """, count + i));
        }

        Request writeToRestoredRequest = new Request("POST", "/restored_" + index + "/_bulk");

        writeToRestoredRequest.addParameter("refresh", "true");
        writeToRestoredRequest.setJsonEntity(bulk.toString());
        assertThat(EntityUtils.toString(client().performRequest(writeToRestoredRequest).getEntity()), containsString("\"errors\":false"));

        // And count to make sure the add worked
        // Make sure search finds all documents
        Request countAfterWriteRequest = new Request("GET", "/restored_" + index + "/_search");
        countAfterWriteRequest.addParameter("size", "0");
        Map<String, Object> countAfterResponse = entityAsMap(client().performRequest(countRequest));
        assertTotalHits(count + extras, countAfterResponse);

        // Clean up the index for the next iteration
        client().performRequest(new Request("DELETE", "/restored_*"));

        // Check settings added by the restore process
        Request clusterSettingsRequest = new Request("GET", "/_cluster/settings");
        clusterSettingsRequest.addParameter("flat_settings", "true");
        Map<String, Object> clusterSettingsResponse = entityAsMap(client().performRequest(clusterSettingsRequest));
        @SuppressWarnings("unchecked")
        final Map<String, Object> persistentSettings = (Map<String, Object>) clusterSettingsResponse.get("persistent");
        assertThat(persistentSettings.get("cluster.routing.allocation.exclude.test_attr"), equalTo(getOldClusterVersion()));

        // Check that the template was restored successfully
        Request getTemplateRequest = new Request("GET", "/_template/test_template");

        Map<String, Object> getTemplateResponse = entityAsMap(client().performRequest(getTemplateRequest));
        Map<String, Object> expectedTemplate = new HashMap<>();
        expectedTemplate.put("index_patterns", singletonList("evil_*"));

        expectedTemplate.put("settings", singletonMap("index", singletonMap("number_of_shards", "1")));
        expectedTemplate.put("mappings", singletonMap("_source", singletonMap("enabled", true)));

        expectedTemplate.put("order", 0);
        Map<String, Object> aliases = new HashMap<>();
        aliases.put("alias1", emptyMap());
        aliases.put("alias2", singletonMap("filter", singletonMap("term", singletonMap("version", tookOnVersion))));
        expectedTemplate.put("aliases", aliases);
        expectedTemplate = singletonMap("test_template", expectedTemplate);
        if (false == expectedTemplate.equals(getTemplateResponse)) {
            NotEqualMessageBuilder builder = new NotEqualMessageBuilder();
            builder.compareMaps(getTemplateResponse, expectedTemplate);
            logger.info("expected: {}\nactual:{}", expectedTemplate, getTemplateResponse);
            fail("template doesn't match:\n" + builder);
        }
    }

    private void indexRandomDocuments(
        final int count,
        final boolean flushAllowed,
        final boolean saveInfo,
        final boolean specifyId,
        final CheckedFunction<Integer, XContentBuilder, IOException> docSupplier
    ) throws IOException {
        logger.info("Indexing {} random documents", count);
        for (int i = 0; i < count; i++) {
            logger.debug("Indexing document [{}]", i);
            Request createDocument = new Request("POST", "/" + index + "/_doc/" + (specifyId ? i : ""));
            createDocument.setJsonEntity(Strings.toString(docSupplier.apply(i)));
            client().performRequest(createDocument);
            if (rarely()) {
                refreshAllIndices();
            }
            if (flushAllowed && rarely()) {
                logger.debug("Flushing [{}]", index);
                client().performRequest(new Request("POST", "/" + index + "/_flush"));
            }
        }
        if (saveInfo) {
            saveInfoDocument(index + "_count", Integer.toString(count));
        }
    }

    private void indexDocument(String id) throws IOException {
        final var req = newXContentRequest(HttpMethod.POST, "/" + index + "/" + "_doc/" + id, (builder, params) -> builder.field("f", "v"));
        assertOK(client().performRequest(req));
    }

    private int countOfIndexedRandomDocuments() throws IOException {
        return Integer.parseInt(loadInfoDocument(index + "_count"));
    }

    private void saveInfoDocument(String id, String value) throws IOException {
        // Only create the first version so we know how many documents are created when the index is first created
        Request request = newXContentRequest(HttpMethod.PUT, "/info/_doc/" + id, (builder, params) -> builder.field("value", value));
        request.addParameter("op_type", "create");
        client().performRequest(request);
    }

    private String loadInfoDocument(String id) throws IOException {
        Request request = new Request("GET", "/info/_doc/" + id);
        request.addParameter("filter_path", "_source");
        String doc = toStr(client().performRequest(request));
        Matcher m = Pattern.compile("\"value\":\"(.+)\"").matcher(doc);
        assertTrue(doc, m.find());
        return m.group(1);
    }

    private List<String> dataNodes(String indexName, RestClient client) throws IOException {
        Request request = new Request("GET", indexName + "/_stats");
        request.addParameter("level", "shards");
        Response response = client.performRequest(request);
        List<String> nodes = new ArrayList<>();
        List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + indexName + ".shards.0");
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            nodes.add(nodeId);
        }
        return nodes;
    }

    /**
     * Wait for an index to have green health, waiting longer than
     * {@link ESRestTestCase#ensureGreen}.
     */
    protected void ensureGreenLongWait(String indexName) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + indexName);
        request.addParameter("timeout", "2m");
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_events", "languid");
        request.addParameter("level", "shards");
        Map<String, Object> healthRsp = entityAsMap(client().performRequest(request));
        logger.info("health api response: {}", healthRsp);
        assertEquals("green", healthRsp.get("status"));
        assertFalse((Boolean) healthRsp.get("timed_out"));
    }

    public void testPeerRecoveryRetentionLeases() throws Exception {
        if (isRunningAgainstOldCluster()) {
            client().performRequest(newXContentRequest(HttpMethod.PUT, "/" + index, (settings, params) -> {
                settings.startObject("settings");
                settings.field("number_of_shards", between(1, 5));
                settings.field("number_of_replicas", between(0, 1));
                settings.endObject();
                return settings;
            }));
        }
        ensureGreen(index);
        ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
    }

    /**
     * Tests that with or without soft-deletes, we should perform an operation-based recovery if there were some
     * but not too many uncommitted documents (i.e., less than 10% of committed documents or the extra translog)
     * before we restart the cluster. This is important when we move from translog based to retention leases based
     * peer recoveries.
     */
    public void testOperationBasedRecovery() throws Exception {
        if (isRunningAgainstOldCluster()) {
            Settings.Builder settings = indexSettings(1, 1);
            if (minimumIndexVersion().before(IndexVersions.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            final String mappings = randomBoolean() ? "\"_source\": { \"enabled\": false}" : null;
            createIndex(index, settings.build(), mappings);
            ensureGreen(index);
            int committedDocs = randomIntBetween(100, 200);
            for (int i = 0; i < committedDocs; i++) {
                indexDocument(Integer.toString(i));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
            flush(index, true);
            ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
            // less than 10% of the committed docs (see IndexSetting#FILE_BASED_RECOVERY_THRESHOLD_SETTING).
            int uncommittedDocs = randomIntBetween(0, (int) (committedDocs * 0.1));
            for (int i = 0; i < uncommittedDocs; i++) {
                final String id = Integer.toString(randomIntBetween(1, 100));
                indexDocument(id);
            }
        } else {
            ensureGreen(index);
            assertNoFileBasedRecovery(index, n -> true);
            ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
        }
    }

    /**
     * Verifies that once all shard copies on the new version, we should turn off the translog retention for indices with soft-deletes.
     */
    public void testTurnOffTranslogRetentionAfterUpgraded() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createIndex(index, indexSettings(1, 1).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
            ensureGreen(index);
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                indexDocument(Integer.toString(randomIntBetween(1, 100)));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
        } else {
            ensureGreen(index);
            flush(index, true);
            assertEmptyTranslog(index);
            ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
        }
    }

    public void testResize() throws Exception {
        int numDocs;
        if (isRunningAgainstOldCluster()) {
            final Settings.Builder settings = indexSettings(3, 1);
            if (minimumIndexVersion().before(IndexVersions.V_8_0_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false);
            }
            final String mappings = randomBoolean() ? "\"_source\": { \"enabled\": false}" : null;
            createIndex(index, settings.build(), mappings);
            numDocs = randomIntBetween(10, 1000);
            for (int i = 0; i < numDocs; i++) {
                indexDocument(Integer.toString(i));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
            saveInfoDocument("num_doc_" + index, Integer.toString(numDocs));
            ensureGreen(index);
        } else {
            ensureGreen(index);
            numDocs = Integer.parseInt(loadInfoDocument("num_doc_" + index));
            int moreDocs = randomIntBetween(0, 100);
            for (int i = 0; i < moreDocs; i++) {
                indexDocument(Integer.toString(numDocs + i));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
            final ToXContent settings0 = (builder, params) -> builder.startObject("settings").field("index.blocks.write", true).endObject();
            client().performRequest(newXContentRequest(HttpMethod.PUT, "/" + index + "/_settings", settings0));
            {
                final String target = index + "_shrunken";
                Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
                if (randomBoolean()) {
                    settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
                }
                client().performRequest(newXContentRequest(HttpMethod.PUT, "/" + index + "/_shrink/" + target, (builder, params) -> {
                    builder.startObject("settings");
                    settings.build().toXContent(builder, params);
                    return builder.endObject();
                }));
                ensureGreenLongWait(target);
                assertNumHits(target, numDocs + moreDocs, 1);
            }
            {
                final String target = index + "_split";
                Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6);
                if (randomBoolean()) {
                    settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
                }
                client().performRequest(newXContentRequest(HttpMethod.PUT, "/" + index + "/_split/" + target, (builder, params) -> {
                    builder.startObject("settings");
                    settings.build().toXContent(builder, params);
                    return builder.endObject();
                }));
                ensureGreenLongWait(target);
                assertNumHits(target, numDocs + moreDocs, 6);
            }
            {
                final String target = index + "_cloned";
                client().performRequest(new Request("PUT", "/" + index + "/_clone/" + target));
                ensureGreenLongWait(target);
                assertNumHits(target, numDocs + moreDocs, 3);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testSystemIndexMetadataIsUpgraded() throws Exception {
        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct "
            + "access to system indices will be prevented by default";
        if (isRunningAgainstOldCluster()) {
            // create index
            client().performRequest(
                newXContentRequest(
                    HttpMethod.PUT,
                    "/test_index_old",
                    (builder, params) -> builder.startObject("settings").field("index.number_of_replicas", 0).endObject()
                )
            );

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("""
                {"index": {"_index": "test_index_old"}}
                {"f1": "v1", "f2": "v2"}
                """);
            client().performRequest(bulk);

            // start a async reindex job
            Request reindex = newXContentRequest(
                HttpMethod.POST,
                "/_reindex",
                (builder, params) -> builder.startObject("source")
                    .field("index", "test_index_old")
                    .endObject()
                    .startObject("dest")
                    .field("index", "test_index_reindex")
                    .endObject()
            );
            reindex.addParameter("wait_for_completion", "false");
            Map<String, Object> response = entityAsMap(client().performRequest(reindex));
            String taskId = (String) response.get("task");

            // wait for task
            Request getTask = new Request("GET", "/_tasks/" + taskId);
            getTask.addParameter("wait_for_completion", "true");
            client().performRequest(getTask);

            // make sure .tasks index exists
            Request getTasksIndex = new Request("GET", "/.tasks");
            getTasksIndex.setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
            getTasksIndex.addParameter("allow_no_indices", "false");

            getTasksIndex.setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
            assertBusy(() -> {
                try {
                    assertThat(client().performRequest(getTasksIndex).getStatusLine().getStatusCode(), is(200));
                } catch (ResponseException e) {
                    throw new AssertionError(".tasks index does not exist yet");
                }
            });
        } else {
            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
                Map<String, Object> indices = new XContentTestUtils.JsonMapView(entityAsMap(client().performRequest(clusterStateRequest)))
                    .get("metadata.indices");

                // Make sure our non-system index is still non-system
                assertThat(new XContentTestUtils.JsonMapView(indices).get("test_index_old.system"), is(false));

                // Can't get the .tasks index via JsonMapView because it splits on `.`
                assertThat(indices, hasKey(".tasks"));
                XContentTestUtils.JsonMapView tasksIndex = new XContentTestUtils.JsonMapView((Map<String, Object>) indices.get(".tasks"));
                assertThat(tasksIndex.get("system"), is(true));

                // If .tasks was created in a 7.x version, it should have an alias on it that we need to make sure got upgraded properly.
                final String tasksCreatedVersionString = tasksIndex.get("settings.index.version.created");
                assertThat(tasksCreatedVersionString, notNullValue());
                final IndexVersion tasksCreatedVersion = IndexVersion.fromId(Integer.parseInt(tasksCreatedVersionString));
                if (tasksCreatedVersion.before(SYSTEM_INDEX_ENFORCEMENT_INDEX_VERSION)) {
                    // Verify that the alias survived the upgrade
                    Request getAliasRequest = new Request("GET", "/_alias/test-system-alias");
                    getAliasRequest.setOptions(expectVersionSpecificWarnings(v -> {
                        v.current(systemIndexWarning);
                        v.compatible(systemIndexWarning);
                    }));
                    Map<String, Object> aliasResponse = entityAsMap(client().performRequest(getAliasRequest));
                    assertThat(aliasResponse, hasKey(".tasks"));
                    assertThat(aliasResponse, hasKey("test_index_reindex"));
                }
            });
        }
    }

    /**
     * This test ensures that search results on old indices using "persian" analyzer don't change
     * after we introduce Lucene 10
     */
    public void testPersianAnalyzerBWC() throws Exception {
        var originalClusterLegacyPersianAnalyzer = oldClusterHasFeature(SearchFeatures.LUCENE_10_0_0_UPGRADE) == false;
        assumeTrue("Don't run this test if both versions already support stemming", originalClusterLegacyPersianAnalyzer);
        final String indexName = "test_persian_stemmer";
        Settings idxSettings = indexSettings(1, 1).build();
        String mapping = """
                {
                  "properties": {
                    "textfield" : {
                      "type": "text",
                      "analyzer": "persian"
                    }
                  }
                }
            """;

        String query = """
                {
                  "query": {
                    "match": {
                      "textfield": "كتابها"
                    }
                  }
                }
            """;

        if (isRunningAgainstOldCluster()) {
            createIndex(client(), indexName, idxSettings, mapping);
            ensureGreen(indexName);

            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + indexName + "/" + "_doc/1",
                        (builder, params) -> builder.field("textfield", "كتابها")
                    )
                )
            );
            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + indexName + "/" + "_doc/2",
                        (builder, params) -> builder.field("textfield", "كتاب")
                    )
                )
            );
            refresh(indexName);

            assertNumHits(indexName, 2, 1);

            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(1, entityAsMap(client().performRequest(searchRequest)));
        } else {
            // old index should still only return one doc
            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(1, entityAsMap(client().performRequest(searchRequest)));

            String newIndexName = indexName + "_new";
            createIndex(client(), newIndexName, idxSettings, mapping);
            ensureGreen(newIndexName);

            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + newIndexName + "/" + "_doc/1",
                        (builder, params) -> builder.field("textfield", "كتابها")
                    )
                )
            );
            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + newIndexName + "/" + "_doc/2",
                        (builder, params) -> builder.field("textfield", "كتاب")
                    )
                )
            );
            refresh(newIndexName);

            searchRequest = new Request("POST", "/" + newIndexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(2, entityAsMap(client().performRequest(searchRequest)));

            // searching both indices (old and new analysis version) we should get 1 hit from the old and 2 from the new index
            searchRequest = new Request("POST", "/" + indexName + "," + newIndexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(3, entityAsMap(client().performRequest(searchRequest)));
        }
    }

    /**
     * This test ensures that search results on old indices using "romanain" analyzer don't change
     * after we introduce Lucene 10
     */
    public void testRomanianAnalyzerBWC() throws Exception {
        var originalClusterLegacyRomanianAnalyzer = oldClusterHasFeature(SearchFeatures.LUCENE_10_0_0_UPGRADE) == false;
        assumeTrue("Don't run this test if both versions already support stemming", originalClusterLegacyRomanianAnalyzer);
        final String indexName = "test_romanian_stemmer";
        Settings idxSettings = indexSettings(1, 1).build();
        String cedillaForm = "absenţa";
        String commaForm = "absența";

        String mapping = """
                {
                  "properties": {
                    "textfield" : {
                      "type": "text",
                      "analyzer": "romanian"
                    }
                  }
                }
            """;

        // query that uses the cedilla form of "t"
        String query = """
                {
                  "query": {
                    "match": {
                      "textfield": "absenţa"
                    }
                  }
                }
            """;

        if (isRunningAgainstOldCluster()) {
            createIndex(client(), indexName, idxSettings, mapping);
            ensureGreen(indexName);

            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + indexName + "/" + "_doc/1",
                        (builder, params) -> builder.field("textfield", cedillaForm)
                    )
                )
            );
            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + indexName + "/" + "_doc/2",
                        // this doc uses the comma form
                        (builder, params) -> builder.field("textfield", commaForm)
                    )
                )
            );
            refresh(indexName);

            assertNumHits(indexName, 2, 1);

            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(1, entityAsMap(client().performRequest(searchRequest)));
        } else {
            // old index should still only return one doc
            Request searchRequest = new Request("POST", "/" + indexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(1, entityAsMap(client().performRequest(searchRequest)));

            String newIndexName = indexName + "_new";
            createIndex(client(), newIndexName, idxSettings, mapping);
            ensureGreen(newIndexName);

            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + newIndexName + "/" + "_doc/1",
                        (builder, params) -> builder.field("textfield", cedillaForm)
                    )
                )
            );
            assertOK(
                client().performRequest(
                    newXContentRequest(
                        HttpMethod.POST,
                        "/" + newIndexName + "/" + "_doc/2",
                        (builder, params) -> builder.field("textfield", commaForm)
                    )
                )
            );
            refresh(newIndexName);

            searchRequest = new Request("POST", "/" + newIndexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(2, entityAsMap(client().performRequest(searchRequest)));

            // searching both indices (old and new analysis version) we should get 1 hit from the old and 2 from the new index
            searchRequest = new Request("POST", "/" + indexName + "," + newIndexName + "/_search");
            searchRequest.setJsonEntity(query);
            assertTotalHits(3, entityAsMap(client().performRequest(searchRequest)));
        }
    }

    public void testForbidDisableSoftDeletesOnRestore() throws Exception {
        final String snapshot = "snapshot-" + index;
        if (isRunningAgainstOldCluster()) {
            final Settings.Builder settings = indexSettings(1, 1).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
            createIndex(index, settings.build());
            ensureGreen(index);
            int numDocs = randomIntBetween(0, 100);
            indexRandomDocuments(
                numDocs,
                true,
                true,
                randomBoolean(),
                i -> jsonBuilder().startObject().field("field", "value").endObject()
            );
            // create repo
            client().performRequest(newXContentRequest(HttpMethod.PUT, "/_snapshot/repo", (repoConfig, params) -> {
                repoConfig.field("type", "fs");
                repoConfig.startObject("settings");
                repoConfig.field("compress", randomBoolean());
                repoConfig.field("location", repoDirectory.getRoot().getPath());
                repoConfig.endObject();
                return repoConfig;
            }));
            // create snapshot
            Request createSnapshot = newXContentRequest(
                HttpMethod.PUT,
                "/_snapshot/repo/" + snapshot,
                (builder, params) -> builder.field("indices", index)
            );
            createSnapshot.addParameter("wait_for_completion", "true");
            client().performRequest(createSnapshot);
        } else {
            // Restore
            Request restoreRequest = newXContentRequest(
                HttpMethod.POST,
                "/_snapshot/repo/" + snapshot + "/_restore",
                (restoreCommand, params) -> {
                    restoreCommand.field("indices", index);
                    restoreCommand.field("rename_pattern", index);
                    restoreCommand.field("rename_replacement", "restored-" + index);
                    restoreCommand.startObject("index_settings").field("index.soft_deletes.enabled", false).endObject();
                    return restoreCommand;
                }
            );
            restoreRequest.addParameter("wait_for_completion", "true");
            final ResponseException error = expectThrows(ResponseException.class, () -> client().performRequest(restoreRequest));
            assertThat(error.getMessage(), containsString("cannot disable setting [index.soft_deletes.enabled] on restore"));
        }
    }

    public static void assertNumHits(String index, int numHits, int totalShards) throws IOException {
        Map<String, Object> resp = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")));
        assertNoFailures(resp);
        assertThat(XContentMapValues.extractValue("_shards.total", resp), equalTo(totalShards));
        assertThat(XContentMapValues.extractValue("_shards.successful", resp), equalTo(totalShards));
        assertThat(extractTotalHits(resp), equalTo(numHits));
    }
}
