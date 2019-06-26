/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.client.dataframe.transforms.DestConfig;
import org.elasticsearch.client.dataframe.transforms.SourceConfig;
import org.elasticsearch.client.dataframe.transforms.TimeSyncConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataFrameSurvivesUpgradeIT extends AbstractUpgradeTestCase {

    private static final String DATAFRAME_ENDPOINT = "/_data_frame/transforms/";
    private static final String CONTINUOUS_DATA_FRAME_ID = "continuous-data-frame-upgrade-job";
    private static final String CONTINUOUS_DATA_FRAME_SOURCE = "data-frame-upgrade-continuous-source";
    private static final List<String> ENTITIES = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(v -> "user_" + v)
        .collect(Collectors.toList());
    private static final List<TimeValue> BUCKETS = Stream.iterate(1, n -> n + 1)
        .limit(5)
        .map(TimeValue::timeValueSeconds)
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

        switch (CLUSTER_TYPE) {
            case OLD:
                createAndStartContinuousDataFrame();
                break;
            case MIXED:
                verifyContinuousDataFrameHandlesData();
                break;
            case UPGRADED:
                verifyContinuousDataFrameHandlesData();
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
        long totalDocsWritten = 0;
        for (TimeValue bucket : BUCKETS) {
            int docs = randomIntBetween(1, 25);
            putData(CONTINUOUS_DATA_FRAME_SOURCE, docs, bucket, ENTITIES);
            totalDocsWritten += docs * ENTITIES.size();
        }

        DataFrameTransformConfig config = DataFrameTransformConfig.builder()
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setPivotConfig(PivotConfig.builder()
                .setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("stars").field("stars")))
                .setGroups(GroupConfig.builder().groupBy("user_id", TermsGroupSource.builder().setField("user_id").build()).build())
                .build())
            .setDest(DestConfig.builder().setIndex(CONTINUOUS_DATA_FRAME_ID + "_idx").build())
            .setSource(SourceConfig.builder().setIndex(CONTINUOUS_DATA_FRAME_SOURCE).build())
            .setId(CONTINUOUS_DATA_FRAME_ID)
            .build();
        putTransform(CONTINUOUS_DATA_FRAME_ID, config);

        startTransform(CONTINUOUS_DATA_FRAME_ID);
        waitUntilCheckpoint(CONTINUOUS_DATA_FRAME_ID, 1);

        DataFrameTransformStateAndStats stateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);

        assertThat(stateAndStats.getTransformStats().getOutputDocuments(), equalTo(ENTITIES.size()));
        assertThat(stateAndStats.getTransformStats().getNumDocuments(), equalTo(totalDocsWritten));
        assertThat(stateAndStats.getTransformState().getTaskState(), equalTo(DataFrameTransformTaskState.STARTED));
    }

    private void verifyContinuousDataFrameHandlesData() throws Exception {
        // A continuous data frame should automatically become started when it gets assigned to a node
        // if it was assigned to the node that was removed from the cluster
        assertBusy(() -> assertThat(getTransformStats(CONTINUOUS_DATA_FRAME_ID).getTransformState().getTaskState(),
            equalTo(DataFrameTransformTaskState.STARTED)),
            60,
            TimeUnit.SECONDS);

        DataFrameTransformStateAndStats previousStateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);

        // Add a new user and write data to it and all the old users
        List<String> entities = new ArrayList<>(ENTITIES);
        entities.add("user_" + ENTITIES.size() + 1);
        int docs = randomIntBetween(1, 25);
        putData(CONTINUOUS_DATA_FRAME_SOURCE, docs, TimeValue.timeValueSeconds(1), entities);
        long totalDocsWritten = docs * entities.size();

        waitUntilCheckpoint(CONTINUOUS_DATA_FRAME_ID, 2);

        DataFrameTransformStateAndStats stateAndStats = getTransformStats(CONTINUOUS_DATA_FRAME_ID);

        assertThat(stateAndStats.getTransformStats().getOutputDocuments(), equalTo(entities.size()));
        assertThat(stateAndStats.getTransformStats().getNumDocuments(),
            equalTo(totalDocsWritten + previousStateAndStats.getTransformStats().getNumDocuments()));
        assertThat(stateAndStats.getTransformState().getTaskState(),
            equalTo(DataFrameTransformTaskState.STARTED));
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

    private DataFrameTransformStateAndStats getTransformStats(String id) throws IOException {
        final Request getStats = new Request("GET", DATAFRAME_ENDPOINT + id + "/stats");
        Response response = client().performRequest(getStats);
        assertEquals(200, response.getStatusLine().getStatusCode());
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            response.getEntity().getContent())) {
            GetDataFrameTransformStatsResponse resp = GetDataFrameTransformStatsResponse.fromXContent(parser);
            assertThat(resp.getTransformsStateAndStats(), hasSize(1));
            return resp.getTransformsStateAndStats().get(0);
        }
    }

    private void waitUntilCheckpoint(String id, long checkpoint) throws Exception {
        assertBusy(() -> assertThat(getTransformStats(id).getTransformState().getCheckpoint(), equalTo(checkpoint)),
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
            client().performRequest(req);
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
                    .append("\",\"timestamp\":\"")
                    .append(timeStamp)
                    .append("\"}\n");
            }
        }
        bulk.append("\r\n");
        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }
}
