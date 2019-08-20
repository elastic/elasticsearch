/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PreviewDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats;
import org.elasticsearch.client.dataframe.transforms.DestConfig;
import org.elasticsearch.client.dataframe.transforms.SourceConfig;
import org.elasticsearch.client.dataframe.transforms.TimeSyncConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.TermsGroupSource;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class DataFrameTransformIT extends ESRestHighLevelClientTestCase {

    private List<String> transformsToClean = new ArrayList<>();

    private void createIndex(String indexName) throws IOException {

        XContentBuilder builder = jsonBuilder();
        builder.startObject()
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

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.mapping(builder);
        CreateIndexResponse response = highLevelClient().indices().create(request, RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());
    }

    private void indexData(String indexName) throws IOException {
        BulkRequest request = new BulkRequest();
        {
            Map<String, Object> doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T12:00:00+00");
            doc.put("user_id", "theresa");
            doc.put("stars", 2);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T18:00:00+00");
            doc.put("user_id", "theresa");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T12:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 5);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-10T18:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));

            doc = new HashMap<>();
            doc.put("timestamp", "2019-03-11T12:00:00+00");
            doc.put("user_id", "michel");
            doc.put("stars", 3);
            request.add(new IndexRequest(indexName).source(doc, XContentType.JSON));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        BulkResponse response = highLevelClient().bulk(request, RequestOptions.DEFAULT);
        assertFalse(response.hasFailures());
    }

    @After
    public void cleanUpTransforms() throws Exception {
        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().stopDataFrameTransform(
                    new StopDataFrameTransformRequest(transformId, Boolean.TRUE, null), RequestOptions.DEFAULT);
        }

        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().deleteDataFrameTransform(
                    new DeleteDataFrameTransformRequest(transformId), RequestOptions.DEFAULT);
        }

        transformsToClean = new ArrayList<>();
        waitForPendingTasks(adminClient());
    }

    public void testCreateDelete() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        String id = "test-crud";
        DataFrameTransformConfig transform = validDataFrameTransformConfig(id, sourceIndex, "pivot-dest");

        DataFrameClient client = highLevelClient().dataFrame();
        AcknowledgedResponse ack = execute(new PutDataFrameTransformRequest(transform), client::putDataFrameTransform,
                client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        ack = execute(new DeleteDataFrameTransformRequest(transform.getId()), client::deleteDataFrameTransform,
                client::deleteDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        // The second delete should fail
        ElasticsearchStatusException deleteError = expectThrows(ElasticsearchStatusException.class,
                () -> execute(new DeleteDataFrameTransformRequest(transform.getId()), client::deleteDataFrameTransform,
                        client::deleteDataFrameTransformAsync));
        assertThat(deleteError.getMessage(), containsString("Transform with id [test-crud] could not be found"));
    }

    public void testUpdate() throws IOException {
        String sourceIndex = "update-transform-source";
        createIndex(sourceIndex);

        String id = "test-update";
        DataFrameTransformConfig transform = validDataFrameTransformConfigBuilder(id, sourceIndex, "pivot-dest")
            .setSyncConfig(new TimeSyncConfig("timefield", TimeValue.timeValueSeconds(60)))
            .build();

        DataFrameClient client = highLevelClient().dataFrame();
        AcknowledgedResponse ack = execute(new PutDataFrameTransformRequest(transform), client::putDataFrameTransform,
            client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        String updatedDescription = "my new description";
        DataFrameTransformConfigUpdate update = DataFrameTransformConfigUpdate.builder().setDescription(updatedDescription).build();
        UpdateDataFrameTransformResponse response = execute(
            new UpdateDataFrameTransformRequest(update, id), client::updateDataFrameTransform,
            client::updateDataFrameTransformAsync);
        assertThat(response.getTransformConfiguration().getDescription(), equalTo(updatedDescription));

        ElasticsearchStatusException updateError = expectThrows(ElasticsearchStatusException.class,
            () -> execute(new UpdateDataFrameTransformRequest(update, "missing-transform"), client::updateDataFrameTransform,
                client::updateDataFrameTransformAsync));
        assertThat(updateError.getMessage(), containsString("Transform with id [missing-transform] could not be found"));
    }

    public void testCreateDeleteWithDefer() throws IOException {
        String sourceIndex = "missing-source-index";

        String id = "test-with-defer";
        DataFrameTransformConfig transform = validDataFrameTransformConfig(id, sourceIndex, "pivot-dest");
        DataFrameClient client = highLevelClient().dataFrame();
        PutDataFrameTransformRequest request = new PutDataFrameTransformRequest(transform);
        request.setDeferValidation(true);
        AcknowledgedResponse ack = execute(request, client::putDataFrameTransform, client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());

        ack = execute(new DeleteDataFrameTransformRequest(transform.getId()), client::deleteDataFrameTransform,
            client::deleteDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());
    }

    public void testGetTransform() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        String id = "test-get";
        DataFrameTransformConfig transform = validDataFrameTransformConfig(id, sourceIndex, "pivot-dest");

        DataFrameClient client = highLevelClient().dataFrame();
        putTransform(transform);

        GetDataFrameTransformRequest getRequest = new GetDataFrameTransformRequest(id);
        GetDataFrameTransformResponse getResponse = execute(getRequest, client::getDataFrameTransform,
                client::getDataFrameTransformAsync);
        assertNull(getResponse.getInvalidTransforms());
        assertThat(getResponse.getTransformConfigurations(), hasSize(1));
        assertEquals(transform.getId(), getResponse.getTransformConfigurations().get(0).getId());
    }

    public void testGetAllAndPageTransforms() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        DataFrameClient client = highLevelClient().dataFrame();

        DataFrameTransformConfig transform = validDataFrameTransformConfig("test-get-all-1", sourceIndex, "pivot-dest-1");
        putTransform(transform);

        transform = validDataFrameTransformConfig("test-get-all-2", sourceIndex, "pivot-dest-2");
        putTransform(transform);

        GetDataFrameTransformRequest getRequest = new GetDataFrameTransformRequest("_all");
        GetDataFrameTransformResponse getResponse = execute(getRequest, client::getDataFrameTransform,
                client::getDataFrameTransformAsync);
        assertNull(getResponse.getInvalidTransforms());
        assertThat(getResponse.getTransformConfigurations(), hasSize(2));
        assertEquals(transform.getId(), getResponse.getTransformConfigurations().get(1).getId());

        getRequest.setPageParams(new PageParams(0,1));
        getResponse = execute(getRequest, client::getDataFrameTransform,
                client::getDataFrameTransformAsync);
        assertNull(getResponse.getInvalidTransforms());
        assertThat(getResponse.getTransformConfigurations(), hasSize(1));

        GetDataFrameTransformRequest getMulitple = new GetDataFrameTransformRequest("test-get-all-1", "test-get-all-2");
        getResponse = execute(getMulitple, client::getDataFrameTransform,
                client::getDataFrameTransformAsync);
        assertNull(getResponse.getInvalidTransforms());
        assertThat(getResponse.getTransformConfigurations(), hasSize(2));
    }

    public void testGetMissingTransform() {
        DataFrameClient client = highLevelClient().dataFrame();

        ElasticsearchStatusException missingError = expectThrows(ElasticsearchStatusException.class,
                () -> execute(new GetDataFrameTransformRequest("unknown"), client::getDataFrameTransform,
                        client::getDataFrameTransformAsync));
        assertThat(missingError.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testStartStop() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);

        String id = "test-stop-start";
        DataFrameTransformConfig transform = validDataFrameTransformConfig(id, sourceIndex, "pivot-dest");

        DataFrameClient client = highLevelClient().dataFrame();
        putTransform(transform);

        StartDataFrameTransformRequest startRequest = new StartDataFrameTransformRequest(id);
        StartDataFrameTransformResponse startResponse =
                execute(startRequest, client::startDataFrameTransform, client::startDataFrameTransformAsync);
        assertTrue(startResponse.isAcknowledged());
        assertThat(startResponse.getNodeFailures(), empty());
        assertThat(startResponse.getTaskFailures(), empty());

        GetDataFrameTransformStatsResponse statsResponse = execute(new GetDataFrameTransformStatsRequest(id),
                client::getDataFrameTransformStats, client::getDataFrameTransformStatsAsync);
        assertThat(statsResponse.getTransformsStats(), hasSize(1));
        DataFrameTransformStats.State taskState = statsResponse.getTransformsStats().get(0).getState();

        // Since we are non-continuous, the transform could auto-stop between being started earlier and us gathering the statistics
        assertThat(taskState, oneOf(DataFrameTransformStats.State.STARTED, DataFrameTransformStats.State.INDEXING,
            DataFrameTransformStats.State.STOPPING, DataFrameTransformStats.State.STOPPED));

        StopDataFrameTransformRequest stopRequest = new StopDataFrameTransformRequest(id, Boolean.TRUE, null);
        StopDataFrameTransformResponse stopResponse =
                execute(stopRequest, client::stopDataFrameTransform, client::stopDataFrameTransformAsync);
        assertTrue(stopResponse.isAcknowledged());
        assertThat(stopResponse.getNodeFailures(), empty());
        assertThat(stopResponse.getTaskFailures(), empty());

        // Calling stop with wait_for_completion assures that we will be in the `STOPPED` state for the transform task
        statsResponse = execute(new GetDataFrameTransformStatsRequest(id),
            client::getDataFrameTransformStats, client::getDataFrameTransformStatsAsync);
        taskState = statsResponse.getTransformsStats().get(0).getState();
        assertThat(taskState, is(DataFrameTransformStats.State.STOPPED));
    }

    @SuppressWarnings("unchecked")
    public void testPreview() throws IOException {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);
        indexData(sourceIndex);

        DataFrameTransformConfig transform = validDataFrameTransformConfig("test-preview", sourceIndex, null);

        DataFrameClient client = highLevelClient().dataFrame();
        PreviewDataFrameTransformResponse preview = execute(new PreviewDataFrameTransformRequest(transform),
                client::previewDataFrameTransform,
                client::previewDataFrameTransformAsync);

        List<Map<String, Object>> docs = preview.getDocs();
        assertThat(docs, hasSize(2));
        Optional<Map<String, Object>> theresa = docs.stream().filter(doc -> "theresa".equals(doc.get("reviewer"))).findFirst();
        assertTrue(theresa.isPresent());
        assertEquals(2.5d, (double) theresa.get().get("avg_rating"), 0.01d);

        Optional<Map<String, Object>> michel = docs.stream().filter(doc -> "michel".equals(doc.get("reviewer"))).findFirst();
        assertTrue(michel.isPresent());
        assertEquals(3.6d, (double) michel.get().get("avg_rating"), 0.1d);

        Map<String, Object> mappings = preview.getMappings();
        assertThat(mappings, hasKey("properties"));
        Map<String, Object> fields = (Map<String, Object>)mappings.get("properties");
        assertThat(fields.get("reviewer"), equalTo(Map.of("type", "keyword")));
        assertThat(fields.get("avg_rating"), equalTo(Map.of("type", "double")));
    }

    private DataFrameTransformConfig validDataFrameTransformConfig(String id, String source, String destination) {
        return validDataFrameTransformConfigBuilder(id, source, destination).build();
    }

    private DataFrameTransformConfig.Builder validDataFrameTransformConfigBuilder(String id, String source, String destination) {
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregations(aggBuilder).build();

        DestConfig destConfig = (destination != null) ? DestConfig.builder().setIndex(destination).build() : null;

        return DataFrameTransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder().setIndex(source).setQuery(new MatchAllQueryBuilder()).build())
            .setDest(destConfig)
            .setPivotConfig(pivotConfig)
            .setDescription("this is a test transform");
    }

    // TODO add tests to cover continuous situations
    public void testGetStats() throws Exception {
        String sourceIndex = "transform-source";
        createIndex(sourceIndex);
        indexData(sourceIndex);

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregations(aggBuilder).build();

        String id = "test-get-stats";
        DataFrameTransformConfig transform = DataFrameTransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder().setIndex(sourceIndex).setQuery(new MatchAllQueryBuilder()).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .setDescription("transform for testing stats")
            .build();

        DataFrameClient client = highLevelClient().dataFrame();
        putTransform(transform);

        GetDataFrameTransformStatsResponse statsResponse = execute(new GetDataFrameTransformStatsRequest(id),
                client::getDataFrameTransformStats, client::getDataFrameTransformStatsAsync);

        assertEquals(1, statsResponse.getTransformsStats().size());
        DataFrameTransformStats stats = statsResponse.getTransformsStats().get(0);
        assertEquals(DataFrameTransformStats.State.STOPPED, stats.getState());

        DataFrameIndexerTransformStats zeroIndexerStats = new DataFrameIndexerTransformStats(
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0.0,
            0.0,
            0.0);
        assertEquals(zeroIndexerStats, stats.getIndexerStats());

        // start the transform
        StartDataFrameTransformResponse startTransformResponse = execute(new StartDataFrameTransformRequest(id),
            client::startDataFrameTransform,
            client::startDataFrameTransformAsync);
        assertThat(startTransformResponse.isAcknowledged(), is(true));
        assertBusy(() -> {
            GetDataFrameTransformStatsResponse response = execute(new GetDataFrameTransformStatsRequest(id),
                    client::getDataFrameTransformStats, client::getDataFrameTransformStatsAsync);
            DataFrameTransformStats stateAndStats = response.getTransformsStats().get(0);
            assertNotEquals(zeroIndexerStats, stateAndStats.getIndexerStats());
            assertThat(stateAndStats.getState(), oneOf(DataFrameTransformStats.State.STARTED, DataFrameTransformStats.State.INDEXING,
                DataFrameTransformStats.State.STOPPING, DataFrameTransformStats.State.STOPPED));
            assertThat(stateAndStats.getReason(), is(nullValue()));
        });
    }

    void putTransform(DataFrameTransformConfig config) throws IOException {
        DataFrameClient client = highLevelClient().dataFrame();
        AcknowledgedResponse ack = execute(new PutDataFrameTransformRequest(config), client::putDataFrameTransform,
            client::putDataFrameTransformAsync);
        assertTrue(ack.isAcknowledged());
        transformsToClean.add(config.getId());
    }
}

