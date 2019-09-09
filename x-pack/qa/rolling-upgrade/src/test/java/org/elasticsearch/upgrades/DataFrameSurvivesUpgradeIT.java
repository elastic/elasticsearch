/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats;
import org.elasticsearch.client.dataframe.transforms.DestConfig;
import org.elasticsearch.client.dataframe.transforms.SourceConfig;
import org.elasticsearch.client.dataframe.transforms.TimeSyncConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class DataFrameSurvivesUpgradeIT extends AbstractUpgradeTestCase {

    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    private static final String DATAFRAME_ENDPOINT = "/_data_frame/transforms/";
    private static final String CONTINUOUS_DATA_FRAME_ID = "continuous-data-frame-upgrade-job";
    private static final String CONTINUOUS_DATA_FRAME_SOURCE = "data-frame-upgrade-continuous-source";
    private static final List<String> ENTITIES = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(v -> "user_" + v)
        .collect(Collectors.toList());
    private static final List<TimeValue> BUCKETS = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(TimeValue::timeValueMinutes)
        .collect(Collectors.toList());

    @Override
    protected Collection<String> templatesToWaitFor() {
        return Stream.concat(XPackRestTestConstants.DATA_FRAME_TEMPLATES.stream(),
            super.templatesToWaitFor().stream()).collect(Collectors.toSet());
    }

    protected static void waitForPendingDataFrameTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith("data_frame/transforms") == false);
    }

    /**
     * The purpose of this test is to ensure that when a job is open through a rolling upgrade we upgrade the results
     * index mappings when it is assigned to an upgraded node even if no other ML endpoint is called after the upgrade
     */
    public void testDataFramesRollingUpgrade() throws Exception {
        assumeTrue("Continuous data frames not supported until 7.3", UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_3_0));
        Request adjustLoggingLevels = new Request("PUT", "/_cluster/settings");
        adjustLoggingLevels.setJsonEntity(
            "{\"transient\": {" +
                "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"trace\"," +
                "\"logger.org.elasticsearch.xpack.dataframe\": \"trace\"}}");
        client().performRequest(adjustLoggingLevels);
        Request waitForYellow = new Request("GET", "/_cluster/health");
        waitForYellow.addParameter("wait_for_nodes", "3");
        waitForYellow.addParameter("wait_for_status", "yellow");
        switch (CLUSTER_TYPE) {
            case OLD:
                client().performRequest(waitForYellow);
                createAndStartContinuousDataFrame();
                break;
            case MIXED:
                client().performRequest(waitForYellow);
                long lastCheckpoint = 1;
                if (Booleans.parseBoolean(System.getProperty("tests.first_round")) == false) {
                    lastCheckpoint = 2;
                }
                verifyContinuousDataFrameHandlesData(lastCheckpoint);
                break;
            case UPGRADED:
                client().performRequest(waitForYellow);
                verifyContinuousDataFrameHandlesData(3);
                cleanUpTransforms();
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void cleanUpTransforms() throws Exception {
        stopTransform(CONTINUOUS_DATA_FRAME_ID);
        deleteTransform(CONTINUOUS_DATA_FRAME_ID);
        waitForPendingDataFrameTasks();
    }

    private void createAndStartContinuousDataFrame() throws Exception {
        createIndex(CONTINUOUS_DATA_FRAME_SOURCE);
        long totalDocsWrittenSum = 0;
        for (TimeValue bucket : BUCKETS) {
            int docs = randomIntBetween(1, 25);
            putData(CONTINUOUS_DATA_FRAME_SOURCE, docs, bucket, ENTITIES);
            totalDocsWrittenSum += docs * ENTITIES.size();
        }
        long totalDocsWritten = totalDocsWrittenSum;
        DataFrameTransformConfig config = DataFrameTransformConfig.builder()
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setPivotConfig(PivotConfig.builder()
                .setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("stars").field("stars")))
                .setGroups(GroupConfig.builder().groupBy("user_id", TermsGroupSource.builder().setField("user_id").build()).build())
                .build())
            .setDest(DestConfig.builder().setIndex(CONTINUOUS_DATA_FRAME_ID + "_idx").build())
            .setSource(SourceConfig.builder().setIndex(CONTINUOUS_DATA_FRAME_SOURCE).build())
            .setId(CONTINUOUS_DATA_FRAME_ID)
            .setFrequency(TimeValue.timeValueSeconds(1))
            .build();
        putTransform(CONTINUOUS_DATA_FRAME_ID, config);

        startTransform(CONTINUOUS_DATA_FRAME_ID);
        waitUntilAfterCheckpoint(CONTINUOUS_DATA_FRAME_ID, 0L);

        assertBusy(() -> {
            DataFrameTransformStats stateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);
            assertThat(stateAndStats.getIndexerStats().getOutputDocuments(), equalTo((long)ENTITIES.size()));
            assertThat(stateAndStats.getIndexerStats().getNumDocuments(), equalTo(totalDocsWritten));
            // Even if we get back to started, we may periodically get set back to `indexing` when triggered.
            // Though short lived due to no changes on the source indices, it could result in flaky test behavior
            assertThat(stateAndStats.getState(), oneOf(DataFrameTransformStats.State.STARTED, DataFrameTransformStats.State.INDEXING));
        }, 120, TimeUnit.SECONDS);


        // We want to make sure our latest state is written before we turn the node off, this makes the testing more reliable
        awaitWrittenIndexerState(CONTINUOUS_DATA_FRAME_ID, IndexerState.STARTED.value());
    }

    @SuppressWarnings("unchecked")
    private void verifyContinuousDataFrameHandlesData(long expectedLastCheckpoint) throws Exception {

        // A continuous data frame should automatically become started when it gets assigned to a node
        // if it was assigned to the node that was removed from the cluster
        assertBusy(() -> {
            DataFrameTransformStats stateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);
            assertThat(stateAndStats.getState(), oneOf(DataFrameTransformStats.State.STARTED, DataFrameTransformStats.State.INDEXING));
        },
        120,
        TimeUnit.SECONDS);

        DataFrameTransformStats previousStateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);

        // Add a new user and write data to it
        // This is so we can have more reliable data counts, as writing to existing entities requires
        // rescanning the past data
        List<String> entities = new ArrayList<>(1);
        entities.add("user_" + ENTITIES.size() + expectedLastCheckpoint);
        int docs = 5;
        // Index the data
        // The frequency and delay should see the data once its indexed
        putData(CONTINUOUS_DATA_FRAME_SOURCE, docs, TimeValue.timeValueSeconds(0), entities);

        waitUntilAfterCheckpoint(CONTINUOUS_DATA_FRAME_ID, expectedLastCheckpoint);

        assertBusy(() -> assertThat(
            getTransformStats(CONTINUOUS_DATA_FRAME_ID).getIndexerStats().getNumDocuments(),
            greaterThanOrEqualTo(docs + previousStateAndStats.getIndexerStats().getNumDocuments())),
            120,
            TimeUnit.SECONDS);
        DataFrameTransformStats stateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);

        assertThat(stateAndStats.getState(),
            oneOf(DataFrameTransformStats.State.STARTED, DataFrameTransformStats.State.INDEXING));
        awaitWrittenIndexerState(CONTINUOUS_DATA_FRAME_ID, (responseBody) -> {
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
        Request getStatsDocsRequest = new Request("GET", ".data-frame-internal-*/_search");
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
            client().performRequest(new Request("POST", ".data-frame-internal-*/_refresh"));
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

    private void putTransform(String id, DataFrameTransformConfig config) throws IOException {
        final Request createDataframeTransformRequest = new Request("PUT", DATAFRAME_ENDPOINT + id);
        createDataframeTransformRequest.setJsonEntity(Strings.toString(config));
        Response response = client().performRequest(createDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void deleteTransform(String id) throws IOException {
        Response response = client().performRequest(new Request("DELETE", DATAFRAME_ENDPOINT + id));
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void startTransform(String id) throws IOException  {
        final Request startDataframeTransformRequest = new Request("POST", DATAFRAME_ENDPOINT + id + "/_start");
        Response response = client().performRequest(startDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void stopTransform(String id) throws IOException  {
        final Request stopDataframeTransformRequest = new Request("POST",
            DATAFRAME_ENDPOINT + id + "/_stop?wait_for_completion=true");
        Response response = client().performRequest(stopDataframeTransformRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private DataFrameTransformStats getTransformStats(String id) throws IOException {
        final Request getStats = new Request("GET", DATAFRAME_ENDPOINT + id + "/_stats");
        Response response = client().performRequest(getStats);
        assertEquals(200, response.getStatusLine().getStatusCode());
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            response.getEntity().getContent())) {
            GetDataFrameTransformStatsResponse resp = GetDataFrameTransformStatsResponse.fromXContent(parser);
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
