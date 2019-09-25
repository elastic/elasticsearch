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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.transform.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.transform.GetDataFrameTransformRequest;
import org.elasticsearch.client.transform.GetDataFrameTransformResponse;
import org.elasticsearch.client.transform.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.transform.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.transform.PreviewDataFrameTransformRequest;
import org.elasticsearch.client.transform.PreviewDataFrameTransformResponse;
import org.elasticsearch.client.transform.PutDataFrameTransformRequest;
import org.elasticsearch.client.transform.StartDataFrameTransformRequest;
import org.elasticsearch.client.transform.StartDataFrameTransformResponse;
import org.elasticsearch.client.transform.StopDataFrameTransformRequest;
import org.elasticsearch.client.transform.StopDataFrameTransformResponse;
import org.elasticsearch.client.transform.UpdateDataFrameTransformRequest;
import org.elasticsearch.client.transform.UpdateDataFrameTransformResponse;
import org.elasticsearch.client.transform.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.client.transform.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.transform.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.client.transform.transforms.DataFrameTransformProgress;
import org.elasticsearch.client.transform.transforms.DataFrameTransformStats;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.NodeAttributes;
import org.elasticsearch.client.transform.transforms.QueryConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataFrameTransformDocumentationIT extends ESRestHighLevelClientTestCase {

    private List<String> transformsToClean = new ArrayList<>();

    @After
    public void cleanUpTransforms() throws Exception {
        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().stopDataFrameTransform(
                    new StopDataFrameTransformRequest(transformId, Boolean.TRUE, TimeValue.timeValueSeconds(20)), RequestOptions.DEFAULT);
        }

        for (String transformId : transformsToClean) {
            highLevelClient().dataFrame().deleteDataFrameTransform(
                    new DeleteDataFrameTransformRequest(transformId), RequestOptions.DEFAULT);
        }

        transformsToClean = new ArrayList<>();
        waitForPendingTasks(adminClient());
    }

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

    public void testPutDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-index");

        RestHighLevelClient client = highLevelClient();

        // tag::put-transform-query-config
        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        // end::put-transform-query-config
        // tag::put-transform-source-config
        SourceConfig sourceConfig = SourceConfig.builder()
            .setIndex("source-index")
            .setQueryConfig(queryConfig).build();
        // end::put-transform-source-config
        // tag::put-transform-dest-config
        DestConfig destConfig = DestConfig.builder()
            .setIndex("pivot-destination")
            .setPipeline("my-pipeline").build();
        // end::put-transform-dest-config
        // tag::put-transform-group-config
        GroupConfig groupConfig = GroupConfig.builder()
            .groupBy("reviewer", // <1>
                TermsGroupSource.builder().setField("user_id").build()) // <2>
            .build();
        // end::put-transform-group-config
        // tag::put-transform-agg-config
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(
                AggregationBuilders.avg("avg_rating").field("stars"));  // <1>
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        // end::put-transform-agg-config
        // tag::put-transform-pivot-config
        PivotConfig pivotConfig = PivotConfig.builder()
            .setGroups(groupConfig) // <1>
            .setAggregationConfig(aggConfig) // <2>
            .setMaxPageSearchSize(1000) // <3>
            .build();
        // end::put-transform-pivot-config
        // tag::put-transform-config
        DataFrameTransformConfig transformConfig = DataFrameTransformConfig
            .builder()
            .setId("reviewer-avg-rating") // <1>
            .setSource(sourceConfig) // <2>
            .setDest(destConfig) // <3>
            .setFrequency(TimeValue.timeValueSeconds(15)) // <4>
            .setPivotConfig(pivotConfig) // <5>
            .setDescription("This is my test transform") // <6>
            .build();
        // end::put-transform-config

        {
            // tag::put-transform-request
            PutDataFrameTransformRequest request =
                    new PutDataFrameTransformRequest(transformConfig); // <1>
            request.setDeferValidation(false); // <2>
            // end::put-transform-request

            // tag::put-transform-execute
            AcknowledgedResponse response =
                    client.dataFrame().putDataFrameTransform(
                            request, RequestOptions.DEFAULT);
            // end::put-transform-execute
            transformsToClean.add(request.getConfig().getId());

            assertTrue(response.isAcknowledged());
        }
        {
            DataFrameTransformConfig configWithDifferentId = DataFrameTransformConfig.builder()
                .setId("reviewer-avg-rating2")
                .setSource(transformConfig.getSource())
                .setDest(transformConfig.getDestination())
                .setPivotConfig(transformConfig.getPivotConfig())
                .build();
            PutDataFrameTransformRequest request = new PutDataFrameTransformRequest(configWithDifferentId);

            // tag::put-transform-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                    new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-transform-execute-async
            client.dataFrame().putDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
            transformsToClean.add(request.getConfig().getId());
        }
    }

    public void testUpdateDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();
        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        DataFrameTransformConfig transformConfig = DataFrameTransformConfig.builder()
            .setId("my-transform-to-update")
            .setSource(SourceConfig.builder().setIndex("source-data").setQueryConfig(queryConfig).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .setSyncConfig(new TimeSyncConfig("time-field", TimeValue.timeValueSeconds(120)))
            .build();

        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(transformConfig.getId());

        // tag::update-transform-config
        DataFrameTransformConfigUpdate update = DataFrameTransformConfigUpdate
            .builder()
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .build()) // <1>
            .setDest(DestConfig.builder()
                .setIndex("pivot-dest")
                .build()) // <2>
            .setFrequency(TimeValue.timeValueSeconds(15)) // <3>
            .setSyncConfig(new TimeSyncConfig("time-field",
                TimeValue.timeValueSeconds(120))) // <4>
            .setDescription("This is my updated transform") // <5>
            .build();
        // end::update-transform-config

        {
        // tag::update-transform-request
        UpdateDataFrameTransformRequest request =
            new UpdateDataFrameTransformRequest(
                update, // <1>
                "my-transform-to-update"); // <2>
        request.setDeferValidation(false); // <3>
        // end::update-transform-request

        // tag::update-transform-execute
        UpdateDataFrameTransformResponse response =
            client.dataFrame().updateDataFrameTransform(request,
                RequestOptions.DEFAULT);
        DataFrameTransformConfig updatedConfig =
            response.getTransformConfiguration();
        // end::update-transform-execute

        assertThat(updatedConfig.getDescription(), equalTo("This is my updated transform"));
        }
        {
        UpdateDataFrameTransformRequest request = new UpdateDataFrameTransformRequest(update,
            "my-transform-to-update");

        // tag::update-transform-execute-listener
        ActionListener<UpdateDataFrameTransformResponse> listener =
            new ActionListener<UpdateDataFrameTransformResponse>() {
                @Override
                public void onResponse(UpdateDataFrameTransformResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::update-transform-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::update-transform-execute-async
        client.dataFrame().updateDataFrameTransformAsync(
            request, RequestOptions.DEFAULT, listener); // <1>
        // end::update-transform-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testStartStop() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        DataFrameTransformConfig transformConfig = DataFrameTransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder().setIndex("source-data").setQueryConfig(queryConfig).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();

        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(transformConfig.getId());

        {
            // tag::start-transform-request
            StartDataFrameTransformRequest request =
                    new StartDataFrameTransformRequest("mega-transform");  // <1>
            // end::start-transform-request

            // tag::start-transform-request-options
            request.setTimeout(TimeValue.timeValueSeconds(20));  // <1>
            // end::start-transform-request-options

            // tag::start-transform-execute
            StartDataFrameTransformResponse response =
                    client.dataFrame().startDataFrameTransform(
                            request, RequestOptions.DEFAULT);
            // end::start-transform-execute

            assertTrue(response.isAcknowledged());
        }
        {
            // tag::stop-transform-request
            StopDataFrameTransformRequest request =
                    new StopDataFrameTransformRequest("mega-transform"); // <1>
            // end::stop-transform-request

            // tag::stop-transform-request-options
            request.setWaitForCompletion(Boolean.TRUE);  // <1>
            request.setTimeout(TimeValue.timeValueSeconds(30));  // <2>
            request.setAllowNoMatch(true); // <3>
            // end::stop-transform-request-options

            // tag::stop-transform-execute
            StopDataFrameTransformResponse response =
                    client.dataFrame().stopDataFrameTransform(
                            request, RequestOptions.DEFAULT);
            // end::stop-transform-execute

            assertTrue(response.isAcknowledged());
        }
        {
            // tag::start-transform-execute-listener
            ActionListener<StartDataFrameTransformResponse> listener =
                    new ActionListener<StartDataFrameTransformResponse>() {
                        @Override
                        public void onResponse(
                                StartDataFrameTransformResponse response) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::start-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            StartDataFrameTransformRequest request = new StartDataFrameTransformRequest("mega-transform");
            // tag::start-transform-execute-async
            client.dataFrame().startDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        {
            // tag::stop-transform-execute-listener
            ActionListener<StopDataFrameTransformResponse> listener =
                    new ActionListener<StopDataFrameTransformResponse>() {
                        @Override
                        public void onResponse(
                                StopDataFrameTransformResponse response) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::stop-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            StopDataFrameTransformRequest request = new StopDataFrameTransformRequest("mega-transform");
            // tag::stop-transform-execute-async
            client.dataFrame().stopDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::stop-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        DataFrameTransformConfig transformConfig1 = DataFrameTransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .setQuery(new MatchAllQueryBuilder())
                .build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();
        DataFrameTransformConfig transformConfig2 = DataFrameTransformConfig.builder()
            .setId("mega-transform2")
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .setQuery(new MatchAllQueryBuilder())
                .build())
            .setDest(DestConfig.builder().setIndex("pivot-dest2").build())
            .setPivotConfig(pivotConfig)
            .build();

        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(transformConfig1), RequestOptions.DEFAULT);
        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(transformConfig2), RequestOptions.DEFAULT);

        {
            // tag::delete-transform-request
            DeleteDataFrameTransformRequest request =
                    new DeleteDataFrameTransformRequest("mega-transform"); // <1>
            request.setForce(false); // <2>
            // end::delete-transform-request

            // tag::delete-transform-execute
            AcknowledgedResponse response =
                    client.dataFrame()
                    .deleteDataFrameTransform(request, RequestOptions.DEFAULT);
            // end::delete-transform-execute

            assertTrue(response.isAcknowledged());
        }
        {
            // tag::delete-transform-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                    new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            DeleteDataFrameTransformRequest request = new DeleteDataFrameTransformRequest("mega-transform2");

            // tag::delete-transform-execute-async
            client.dataFrame().deleteDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::delete-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPreview() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        // tag::preview-transform-request
        DataFrameTransformConfig transformConfig =
            DataFrameTransformConfig.forPreview(
                SourceConfig.builder()
                    .setIndex("source-data")
                    .setQueryConfig(queryConfig)
                    .build(), // <1>
                pivotConfig); // <2>

        PreviewDataFrameTransformRequest request =
                new PreviewDataFrameTransformRequest(transformConfig); // <3>
        // end::preview-transform-request

        {
            // tag::preview-transform-execute
            PreviewDataFrameTransformResponse response =
                client.dataFrame()
                    .previewDataFrameTransform(request, RequestOptions.DEFAULT);
            // end::preview-transform-execute

            assertNotNull(response.getDocs());
            assertNotNull(response.getMappings());
        }
        {
            // tag::preview-transform-execute-listener
            ActionListener<PreviewDataFrameTransformResponse> listener =
                new ActionListener<PreviewDataFrameTransformResponse>() {
                    @Override
                    public void onResponse(PreviewDataFrameTransformResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::preview-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::preview-transform-execute-async
            client.dataFrame().previewDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::preview-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetStats() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        String id = "statisitcal-transform";
        DataFrameTransformConfig transformConfig = DataFrameTransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .setQuery(new MatchAllQueryBuilder())
                .build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();
        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(id);

        // tag::get-transform-stats-request
        GetDataFrameTransformStatsRequest request =
                new GetDataFrameTransformStatsRequest(id); // <1>
        // end::get-transform-stats-request

        // tag::get-transform-stats-request-options
        request.setPageParams(new PageParams(0, 100)); // <1>
        request.setAllowNoMatch(true); // <2>
        // end::get-transform-stats-request-options

        {
            // tag::get-transform-stats-execute
            GetDataFrameTransformStatsResponse response =
                client.dataFrame()
                    .getDataFrameTransformStats(request, RequestOptions.DEFAULT);
            // end::get-transform-stats-execute

            assertThat(response.getTransformsStats(), hasSize(1));

            // tag::get-transform-stats-response
            DataFrameTransformStats stats =
                response.getTransformsStats().get(0); // <1>
            DataFrameTransformStats.State state =
                stats.getState(); // <2>
            DataFrameIndexerTransformStats indexerStats =
                stats.getIndexerStats(); // <3>
            DataFrameTransformProgress progress =
                stats.getCheckpointingInfo()
                    .getNext().getCheckpointProgress(); // <4>
            NodeAttributes node =
                stats.getNode(); // <5>
            // end::get-transform-stats-response

            assertEquals(DataFrameTransformStats.State.STOPPED, state);
            assertNotNull(indexerStats);
            assertNull(progress);
        }
        {
            // tag::get-transform-stats-execute-listener
            ActionListener<GetDataFrameTransformStatsResponse> listener =
                    new ActionListener<GetDataFrameTransformStatsResponse>() {
                        @Override
                        public void onResponse(
                                GetDataFrameTransformStatsResponse response) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::get-transform-stats-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-transform-stats-execute-async
            client.dataFrame().getDataFrameTransformStatsAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::get-transform-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }


    public void testGetDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
            TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();


        DataFrameTransformConfig putTransformConfig = DataFrameTransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .setQuery(new MatchAllQueryBuilder())
                .build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();

        RestHighLevelClient client = highLevelClient();
        client.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(putTransformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(putTransformConfig.getId());

        {
            // tag::get-transform-request
            GetDataFrameTransformRequest request =
                    new GetDataFrameTransformRequest("mega-transform"); // <1>
            // end::get-transform-request

            // tag::get-transform-request-options
            request.setPageParams(new PageParams(0, 100)); // <1>
            request.setAllowNoMatch(true); // <2>
            // end::get-transform-request-options

            // tag::get-transform-execute
            GetDataFrameTransformResponse response =
                client.dataFrame()
                    .getDataFrameTransform(request, RequestOptions.DEFAULT);
            // end::get-transform-execute

            // tag::get-transform-response
            List<DataFrameTransformConfig> transformConfigs =
                    response.getTransformConfigurations();
            // end::get-transform-response

            assertEquals(1, transformConfigs.size());
        }
        {
            // tag::get-transform-execute-listener
            ActionListener<GetDataFrameTransformResponse> listener =
                new ActionListener<GetDataFrameTransformResponse>() {
                    @Override
                    public void onResponse(GetDataFrameTransformResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-transform-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            GetDataFrameTransformRequest request = new GetDataFrameTransformRequest("mega-transform");

            // tag::get-transform-execute-async
            client.dataFrame().getDataFrameTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::get-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
