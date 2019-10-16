/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_INTERNAL_INDEX_PREFIX;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_NOTIFICATIONS_INDEX_PREFIX;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_NOTIFICATIONS_INDEX_PREFIX_DEPRECATED;
import static org.elasticsearch.xpack.test.rest.XPackRestTestConstants.TRANSFORM_TASK_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class TransformSurvivesUpgradeIT extends AbstractUpgradeTestCase {

    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    private static final String TRANSFORM_ENDPOINT = "/_transform/";
    private static final String TRANSFORM_ENDPOINT_DEPRECATED = "/_data_frame/transforms/";
    private static final String CONTINUOUS_TRANSFORM_ID = "continuous-transform-upgrade-job";
    private static final String CONTINUOUS_TRANSFORM_SOURCE = "transform-upgrade-continuous-source";
    private static final List<String> ENTITIES = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(v -> "user_" + v)
        .collect(Collectors.toList());
    private static final List<TimeValue> BUCKETS = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(TimeValue::timeValueMinutes)
        .collect(Collectors.toList());

    @Before
    public void waitForTemplates() throws Exception {
        assertBusy(() -> {
            final Request catRequest = new Request("GET", "_cat/templates?h=n&s=n");
            final Response catResponse = adminClient().performRequest(catRequest);

            final SortedSet<String> templates = new TreeSet<>(Streams.readAllLines(catResponse.getEntity().getContent()));

            // match templates, independent of the version, at least 2 should exist
            SortedSet<String> internalDeprecated = templates.tailSet(TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED);
            SortedSet<String> internal = templates.tailSet(TRANSFORM_INTERNAL_INDEX_PREFIX);
            SortedSet<String> notificationsDeprecated = templates
                    .tailSet(TRANSFORM_NOTIFICATIONS_INDEX_PREFIX_DEPRECATED);
            SortedSet<String> notifications = templates.tailSet(TRANSFORM_NOTIFICATIONS_INDEX_PREFIX);

            int foundTemplates = 0;
            foundTemplates += internalDeprecated.isEmpty() ? 0
                    : internalDeprecated.first().startsWith(TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED) ? 1 : 0;
            foundTemplates += internal.isEmpty() ? 0 : internal.first().startsWith(TRANSFORM_INTERNAL_INDEX_PREFIX) ? 1 : 0;
            foundTemplates += notificationsDeprecated.isEmpty() ? 0
                    : notificationsDeprecated.first().startsWith(TRANSFORM_NOTIFICATIONS_INDEX_PREFIX_DEPRECATED) ? 1 : 0;
            foundTemplates += notifications.isEmpty() ? 0 : notifications.first().startsWith(TRANSFORM_NOTIFICATIONS_INDEX_PREFIX) ? 1 : 0;

            if (foundTemplates < 2) {
                fail("Transform index templates not found. The templates that exist are: " + templates);
            }
        });
    }

    protected static void waitForPendingTransformTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(TRANSFORM_TASK_NAME) == false);
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    /**
     * The purpose of this test is to ensure that when a job is open through a rolling upgrade we upgrade the results
     * index mappings when it is assigned to an upgraded node even if no other ML endpoint is called after the upgrade
     */
    public void testTransformRollingUpgrade() throws Exception {
        assumeTrue("Continuous transform not supported until 7.3", UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_3_0));
        Request adjustLoggingLevels = new Request("PUT", "/_cluster/settings");
        adjustLoggingLevels.setJsonEntity(
            "{\"transient\": {" +
                "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"trace\"," +
                "\"logger.org.elasticsearch.xpack.dataframe\": \"trace\"," +
                "\"logger.org.elasticsearch.xpack.transform\": \"trace\"" +
                "}}");
        client().performRequest(adjustLoggingLevels);
        Request waitForYellow = new Request("GET", "/_cluster/health");
        waitForYellow.addParameter("wait_for_nodes", "3");
        waitForYellow.addParameter("wait_for_status", "yellow");
        switch (CLUSTER_TYPE) {
            case OLD:
                client().performRequest(waitForYellow);
                createAndStartContinuousTransform();
                break;
            case MIXED:
                client().performRequest(waitForYellow);
                long lastCheckpoint = 1;
                if (Booleans.parseBoolean(System.getProperty("tests.first_round")) == false) {
                    lastCheckpoint = 2;
                }
                verifyContinuousTransformHandlesData(lastCheckpoint);
                break;
            case UPGRADED:
                client().performRequest(waitForYellow);
                verifyContinuousTransformHandlesData(3);
                cleanUpTransforms();
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void cleanUpTransforms() throws Exception {
        stopTransform(CONTINUOUS_TRANSFORM_ID);
        deleteTransform(CONTINUOUS_TRANSFORM_ID);
        waitForPendingTransformTasks();
    }

    private void createAndStartContinuousTransform() throws Exception {
        createIndex(CONTINUOUS_TRANSFORM_SOURCE);
        long totalDocsWrittenSum = 0;
        for (TimeValue bucket : BUCKETS) {
            int docs = randomIntBetween(1, 25);
            putData(CONTINUOUS_TRANSFORM_SOURCE, docs, bucket, ENTITIES);
            totalDocsWrittenSum += docs * ENTITIES.size();
        }
        long totalDocsWritten = totalDocsWrittenSum;
        TransformConfig config = TransformConfig.builder()
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setPivotConfig(PivotConfig.builder()
                .setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("stars").field("stars")))
                .setGroups(GroupConfig.builder().groupBy("user_id", TermsGroupSource.builder().setField("user_id").build()).build())
                .build())
            .setDest(DestConfig.builder().setIndex(CONTINUOUS_TRANSFORM_ID + "_idx").build())
            .setSource(SourceConfig.builder().setIndex(CONTINUOUS_TRANSFORM_SOURCE).build())
            .setId(CONTINUOUS_TRANSFORM_ID)
            .setFrequency(TimeValue.timeValueSeconds(1))
            .build();
        putTransform(CONTINUOUS_TRANSFORM_ID, config);

        startTransform(CONTINUOUS_TRANSFORM_ID);
        waitUntilAfterCheckpoint(CONTINUOUS_TRANSFORM_ID, 0L);

        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);
            assertThat(stateAndStats.getIndexerStats().getOutputDocuments(), equalTo((long)ENTITIES.size()));
            assertThat(stateAndStats.getIndexerStats().getNumDocuments(), equalTo(totalDocsWritten));
            // Even if we get back to started, we may periodically get set back to `indexing` when triggered.
            // Though short lived due to no changes on the source indices, it could result in flaky test behavior
            assertThat(stateAndStats.getState(), oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        }, 120, TimeUnit.SECONDS);


        // We want to make sure our latest state is written before we turn the node off, this makes the testing more reliable
        awaitWrittenIndexerState(CONTINUOUS_TRANSFORM_ID, IndexerState.STARTED.value());
    }

    @SuppressWarnings("unchecked")
    private void verifyContinuousTransformHandlesData(long expectedLastCheckpoint) throws Exception {

        // A continuous transform should automatically become started when it gets assigned to a node
        // if it was assigned to the node that was removed from the cluster
        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);
            assertThat(stateAndStats.getState(), oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        },
        120,
        TimeUnit.SECONDS);

        TransformStats previousStateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);

        // Add a new user and write data to it
        // This is so we can have more reliable data counts, as writing to existing entities requires
        // rescanning the past data
        List<String> entities = new ArrayList<>(1);
        entities.add("user_" + ENTITIES.size() + expectedLastCheckpoint);
        int docs = 5;
        // Index the data
        // The frequency and delay should see the data once its indexed
        putData(CONTINUOUS_TRANSFORM_SOURCE, docs, TimeValue.timeValueSeconds(0), entities);

        waitUntilAfterCheckpoint(CONTINUOUS_TRANSFORM_ID, expectedLastCheckpoint);

        assertBusy(() -> assertThat(
            getTransformStats(CONTINUOUS_TRANSFORM_ID).getIndexerStats().getNumDocuments(),
            greaterThanOrEqualTo(docs + previousStateAndStats.getIndexerStats().getNumDocuments())),
            120,
            TimeUnit.SECONDS);
        TransformStats stateAndStats = getTransformStats(CONTINUOUS_TRANSFORM_ID);

        assertThat(stateAndStats.getState(),
            oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING));
        awaitWrittenIndexerState(CONTINUOUS_TRANSFORM_ID, (responseBody) -> {
            Map<String, Object> indexerStats = (Map<String,Object>)((List<?>)XContentMapValues.extractValue("hits.hits._source.stats",
                responseBody))
                .get(0);
            assertThat((Integer)indexerStats.get("documents_indexed"),
                greaterThan(Long.valueOf(previousStateAndStats.getIndexerStats().getOutputDocuments()).intValue()));
            assertThat((Integer)indexerStats.get("documents_processed"),
                greaterThan(Long.valueOf(previousStateAndStats.getIndexerStats().getNumDocuments()).intValue()));
        });
    }

    private void awaitWrittenIndexerState(String id, Consumer<Map<?, ?>> responseAssertion) throws Exception {
        Request getStatsDocsRequest = new Request("GET",
            TRANSFORM_INTERNAL_INDEX_PREFIX + "*," +
            TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED + "*" +
            "/_search");

        getStatsDocsRequest.setJsonEntity("{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"filter\": \n" +
            "        {\"term\": {\n" +
            "          \"_id\": \"data_frame_transform_state_and_stats-" + id + "\"\n" +
            "        }}\n" +
            "    }\n" +
            "  },\n" +
            "  \"sort\": [\n" +
            "    {\n" +
            "      \"_index\": {\n" +
            "        \"order\": \"desc\"\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"size\": 1\n" +
            "}");
        assertBusy(() -> {
            // Want to make sure we get the latest docs
            client().performRequest(new Request("POST", TRANSFORM_INTERNAL_INDEX_PREFIX + "*/_refresh"));
            client().performRequest(new Request("POST", TRANSFORM_INTERNAL_INDEX_PREFIX_DEPRECATED + "*/_refresh"));
            Response response = client().performRequest(getStatsDocsRequest);
            assertEquals(200, response.getStatusLine().getStatusCode());
            Map<String, Object> responseBody = entityAsMap(response);
            assertEquals(1, XContentMapValues.extractValue("hits.total.value", responseBody));
            responseAssertion.accept(responseBody);
        }, 60, TimeUnit.SECONDS);
    }

    private void awaitWrittenIndexerState(String id, String indexerState) throws Exception {
        awaitWrittenIndexerState(id, (responseBody) -> {
            String storedState = ((List<?>)XContentMapValues.extractValue("hits.hits._source.state.indexer_state", responseBody))
                .get(0)
                .toString();
            assertThat(storedState, equalTo(indexerState));
        });
    }

    private String getTransformEndpoint() {
        return CLUSTER_TYPE == ClusterType.UPGRADED ? TRANSFORM_ENDPOINT : TRANSFORM_ENDPOINT_DEPRECATED;
    }

    private void putTransform(String id, TransformConfig config) throws IOException {
        final Request createDataframeTransformRequest = new Request("PUT", getTransformEndpoint() + id);
        createDataframeTransformRequest.setJsonEntity(Strings.toString(config));
        Response response = client().performRequest(createDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void deleteTransform(String id) throws IOException {
        Response response = client().performRequest(new Request("DELETE", getTransformEndpoint() + id));
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void startTransform(String id) throws IOException  {
        final Request startDataframeTransformRequest = new Request("POST", getTransformEndpoint() + id + "/_start");
        Response response = client().performRequest(startDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void stopTransform(String id) throws IOException  {
        final Request stopDataframeTransformRequest = new Request("POST",
            getTransformEndpoint() + id + "/_stop?wait_for_completion=true");
        Response response = client().performRequest(stopDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private TransformStats getTransformStats(String id) throws IOException {
        final Request getStats = new Request("GET", getTransformEndpoint() + id + "/_stats");
        Response response = client().performRequest(getStats);
        assertEquals(200, response.getStatusLine().getStatusCode());
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            response.getEntity().getContent())) {
            GetTransformStatsResponse resp = GetTransformStatsResponse.fromXContent(parser);
            assertThat(resp.getTransformsStats(), hasSize(1));
            return resp.getTransformsStats().get(0);
        }
    }

    private void waitUntilAfterCheckpoint(String id, long currentCheckpoint) throws Exception {
        assertBusy(() -> assertThat(getTransformStats(id).getCheckpointingInfo().getLast().getCheckpoint(), greaterThan(currentCheckpoint)),
            60, TimeUnit.SECONDS);
    }

    private void createIndex(String indexName) throws IOException {
        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("properties")
                    .startObject("timestamp")
                    .field("type", "date")
                    .endObject()
                    .startObject("user_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("stars")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject();
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", indexName);
            req.setEntity(entity);
            assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    private void putData(String indexName, int numDocs, TimeValue fromTime, List<String> entityIds) throws IOException {
        long timeStamp = Instant.now().toEpochMilli() - fromTime.getMillis();

        // create index
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            for (String entity : entityIds) {
                bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n")
                    .append("{\"user_id\":\"")
                    .append(entity)
                    .append("\",\"stars\":")
                    .append(randomLongBetween(0, 5))
                    .append(",\"timestamp\":")
                    .append(timeStamp)
                    .append("}\n");
            }
        }
        bulk.append("\r\n");
        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        entityAsMap(client().performRequest(bulkRequest));
    }
}
