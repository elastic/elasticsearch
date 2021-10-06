/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PreviewTransformResponse;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StartTransformResponse;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.StopTransformResponse;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.client.transform.UpdateTransformResponse;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.NodeAttributes;
import org.elasticsearch.client.transform.transforms.QueryConfig;
import org.elasticsearch.client.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.SettingsConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.SyncConfig;
import org.elasticsearch.client.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.client.transform.transforms.TransformIndexerStats;
import org.elasticsearch.client.transform.transforms.TransformProgress;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.client.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.core.TimeValue;
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

public class TransformDocumentationIT extends ESRestHighLevelClientTestCase {

    private List<String> transformsToClean = new ArrayList<>();

    @After
    public void cleanUpTransforms() throws Exception {
        for (String transformId : transformsToClean) {
            highLevelClient().transform()
                .stopTransform(new StopTransformRequest(transformId, true, TimeValue.timeValueSeconds(20), false), RequestOptions.DEFAULT);
        }

        for (String transformId : transformsToClean) {
            highLevelClient().transform().deleteTransform(new DeleteTransformRequest(transformId), RequestOptions.DEFAULT);
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

    public void testPutTransform() throws IOException, InterruptedException {
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
        destConfig = DestConfig.builder().setIndex("pivot-destination").build();
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
            .build();
        // end::put-transform-pivot-config
        // tag::put-transform-settings-config
        SettingsConfig settings = SettingsConfig.builder()
            .setMaxPageSearchSize(1000) // <1>
            .build();
        // end::put-transform-settings-config
        // tag::put-transform-retention-policy-config
        RetentionPolicyConfig retentionPolicy = TimeRetentionPolicyConfig.builder()
            .setField("time-field") // <1>
            .setMaxAge(TimeValue.timeValueDays(30)) // <2>
            .build();
        // end::put-transform-retention-policy-config
        // tag::put-transform-sync-config
        SyncConfig syncConfig = TimeSyncConfig.builder()
            .setField("time-field") // <1>
            .setDelay(TimeValue.timeValueSeconds(30)) // <2>
            .build();
        // end::put-transform-sync-config
        // tag::put-transform-config
        TransformConfig transformConfig = TransformConfig
            .builder()
            .setId("reviewer-avg-rating") // <1>
            .setSource(sourceConfig) // <2>
            .setDest(destConfig) // <3>
            .setFrequency(TimeValue.timeValueSeconds(15)) // <4>
            .setPivotConfig(pivotConfig) // <5>
            .setDescription("This is my test transform") // <6>
            .setSettings(settings) // <7>
            .setRetentionPolicyConfig(retentionPolicy) // <8>
            .setSyncConfig(syncConfig) // <9>
            .build();
        // end::put-transform-config

        {
            // tag::put-transform-request
            PutTransformRequest request =
                    new PutTransformRequest(transformConfig); // <1>
            request.setDeferValidation(false); // <2>
            // end::put-transform-request

            // tag::put-transform-execute
            AcknowledgedResponse response =
                    client.transform().putTransform(
                            request, RequestOptions.DEFAULT);
            // end::put-transform-execute
            transformsToClean.add(request.getConfig().getId());

            assertTrue(response.isAcknowledged());
        }
        {
            TransformConfig configWithDifferentId = TransformConfig.builder()
                .setId("reviewer-avg-rating2")
                .setSource(transformConfig.getSource())
                .setDest(transformConfig.getDestination())
                .setPivotConfig(transformConfig.getPivotConfig())
                .build();
            PutTransformRequest request = new PutTransformRequest(configWithDifferentId);

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
            client.transform().putTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
            transformsToClean.add(request.getConfig().getId());
        }
    }

    public void testUpdateTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();
        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        TransformConfig transformConfig = TransformConfig.builder()
            .setId("my-transform-to-update")
            .setSource(SourceConfig.builder().setIndex("source-data").setQueryConfig(queryConfig).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .setSyncConfig(TimeSyncConfig.builder().setField("time-field").setDelay(TimeValue.timeValueSeconds(120)).build())
            .build();

        client.transform().putTransform(new PutTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(transformConfig.getId());

        // tag::update-transform-config
        TransformConfigUpdate update = TransformConfigUpdate
            .builder()
            .setSource(SourceConfig.builder()
                .setIndex("source-data")
                .build()) // <1>
            .setDest(DestConfig.builder()
                .setIndex("pivot-dest")
                .build()) // <2>
            .setFrequency(TimeValue.timeValueSeconds(15)) // <3>
            .setSyncConfig(TimeSyncConfig.builder()
                .setField("time-field")
                .setDelay(TimeValue.timeValueSeconds(120))
                .build()) // <4>
            .setDescription("This is my updated transform") // <5>
            .setRetentionPolicyConfig(TimeRetentionPolicyConfig.builder()
                .setField("time-field")
                .setMaxAge(TimeValue.timeValueDays(30))
                .build()) // <6>
            .build();
        // end::update-transform-config

        {
        // tag::update-transform-request
        UpdateTransformRequest request =
            new UpdateTransformRequest(
                update, // <1>
                "my-transform-to-update"); // <2>
        request.setDeferValidation(false); // <3>
        // end::update-transform-request

        // tag::update-transform-execute
        UpdateTransformResponse response =
            client.transform().updateTransform(request,
                RequestOptions.DEFAULT);
        TransformConfig updatedConfig =
            response.getTransformConfiguration();
        // end::update-transform-execute

            assertThat(updatedConfig.getDescription(), equalTo("This is my updated transform"));
        }
        {
            UpdateTransformRequest request = new UpdateTransformRequest(update, "my-transform-to-update");

        // tag::update-transform-execute-listener
        ActionListener<UpdateTransformResponse> listener =
            new ActionListener<UpdateTransformResponse>() {
                @Override
                public void onResponse(UpdateTransformResponse response) {
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
        client.transform().updateTransformAsync(
            request, RequestOptions.DEFAULT, listener); // <1>
        // end::update-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testStartStop() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        TransformConfig transformConfig = TransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder().setIndex("source-data").setQueryConfig(queryConfig).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();

        client.transform().putTransform(new PutTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(transformConfig.getId());

        {
            // tag::start-transform-request
            StartTransformRequest request =
                    new StartTransformRequest("mega-transform");  // <1>
            // end::start-transform-request

            // tag::start-transform-request-options
            request.setTimeout(TimeValue.timeValueSeconds(20));  // <1>
            // end::start-transform-request-options

            // tag::start-transform-execute
            StartTransformResponse response =
                    client.transform().startTransform(
                            request, RequestOptions.DEFAULT);
            // end::start-transform-execute

            assertTrue(response.isAcknowledged());
        }
        {
            // tag::stop-transform-request
            StopTransformRequest request =
                    new StopTransformRequest("mega-transform"); // <1>
            // end::stop-transform-request

            // tag::stop-transform-request-options
            request.setWaitForCompletion(Boolean.TRUE);  // <1>
            request.setTimeout(TimeValue.timeValueSeconds(30));  // <2>
            request.setAllowNoMatch(true); // <3>
            // end::stop-transform-request-options

            // tag::stop-transform-execute
            StopTransformResponse response =
                    client.transform().stopTransform(
                            request, RequestOptions.DEFAULT);
            // end::stop-transform-execute

            assertTrue(response.isAcknowledged());
        }
        {
            // tag::start-transform-execute-listener
            ActionListener<StartTransformResponse> listener =
                    new ActionListener<StartTransformResponse>() {
                        @Override
                        public void onResponse(
                                StartTransformResponse response) {
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

            StartTransformRequest request = new StartTransformRequest("mega-transform");
            // tag::start-transform-execute-async
            client.transform().startTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        {
            // tag::stop-transform-execute-listener
            ActionListener<StopTransformResponse> listener =
                    new ActionListener<StopTransformResponse>() {
                        @Override
                        public void onResponse(
                                StopTransformResponse response) {
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

            StopTransformRequest request = new StopTransformRequest("mega-transform");
            // tag::stop-transform-execute-async
            client.transform().stopTransformAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::stop-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        TransformConfig transformConfig1 = TransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder().setIndex("source-data").setQuery(new MatchAllQueryBuilder()).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();
        TransformConfig transformConfig2 = TransformConfig.builder()
            .setId("mega-transform2")
            .setSource(SourceConfig.builder().setIndex("source-data").setQuery(new MatchAllQueryBuilder()).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest2").build())
            .setPivotConfig(pivotConfig)
            .build();

        client.transform().putTransform(new PutTransformRequest(transformConfig1), RequestOptions.DEFAULT);
        client.transform().putTransform(new PutTransformRequest(transformConfig2), RequestOptions.DEFAULT);

        {
            // tag::delete-transform-request
            DeleteTransformRequest request =
                    new DeleteTransformRequest("mega-transform"); // <1>
            request.setForce(false); // <2>
            // end::delete-transform-request

            // tag::delete-transform-execute
            AcknowledgedResponse response =
                    client.transform()
                    .deleteTransform(request, RequestOptions.DEFAULT);
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

            DeleteTransformRequest request = new DeleteTransformRequest("mega-transform2");

            // tag::delete-transform-execute-async
            client.transform().deleteTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::delete-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPreview() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        // tag::preview-transform-request
        TransformConfig transformConfig =
            TransformConfig.forPreview(
                SourceConfig.builder()
                    .setIndex("source-data")
                    .setQueryConfig(queryConfig)
                    .build(), // <1>
                pivotConfig); // <2>

        PreviewTransformRequest request =
                new PreviewTransformRequest(transformConfig); // <3>
        // end::preview-transform-request

        {
            // tag::preview-transform-execute
            PreviewTransformResponse response =
                client.transform()
                    .previewTransform(request, RequestOptions.DEFAULT);
            // end::preview-transform-execute

            assertNotNull(response.getDocs());
            assertNotNull(response.getMappings());
        }
        {
            // tag::preview-transform-execute-listener
            ActionListener<PreviewTransformResponse> listener =
                new ActionListener<PreviewTransformResponse>() {
                    @Override
                    public void onResponse(PreviewTransformResponse response) {
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
            client.transform().previewTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::preview-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetStats() throws IOException, InterruptedException {
        createIndex("source-data");

        RestHighLevelClient client = highLevelClient();

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        String id = "statisitcal-transform";
        TransformConfig transformConfig = TransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder().setIndex("source-data").setQuery(new MatchAllQueryBuilder()).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();
        client.transform().putTransform(new PutTransformRequest(transformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(id);

        // tag::get-transform-stats-request
        GetTransformStatsRequest request =
                new GetTransformStatsRequest(id); // <1>
        // end::get-transform-stats-request

        // tag::get-transform-stats-request-options
        request.setPageParams(new PageParams(0, 100)); // <1>
        request.setAllowNoMatch(true); // <2>
        // end::get-transform-stats-request-options

        {
            // tag::get-transform-stats-execute
            GetTransformStatsResponse response =
                client.transform()
                    .getTransformStats(request, RequestOptions.DEFAULT);
            // end::get-transform-stats-execute

            assertThat(response.getTransformsStats(), hasSize(1));

            // tag::get-transform-stats-response
            TransformStats stats =
                response.getTransformsStats().get(0); // <1>
            TransformStats.State state =
                stats.getState(); // <2>
            TransformIndexerStats indexerStats =
                stats.getIndexerStats(); // <3>
            TransformProgress progress =
                stats.getCheckpointingInfo()
                    .getNext().getCheckpointProgress(); // <4>
            NodeAttributes node =
                stats.getNode(); // <5>
            // end::get-transform-stats-response

            assertEquals(TransformStats.State.STOPPED, state);
            assertNotNull(indexerStats);
            assertNull(progress);
        }
        {
            // tag::get-transform-stats-execute-listener
            ActionListener<GetTransformStatsResponse> listener =
                    new ActionListener<GetTransformStatsResponse>() {
                        @Override
                        public void onResponse(
                                GetTransformStatsResponse response) {
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
            client.transform().getTransformStatsAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::get-transform-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetDataFrameTransform() throws IOException, InterruptedException {
        createIndex("source-data");

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer", TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggConfig = new AggregationConfig(aggBuilder);
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregationConfig(aggConfig).build();

        TransformConfig putTransformConfig = TransformConfig.builder()
            .setId("mega-transform")
            .setSource(SourceConfig.builder().setIndex("source-data").setQuery(new MatchAllQueryBuilder()).build())
            .setDest(DestConfig.builder().setIndex("pivot-dest").build())
            .setPivotConfig(pivotConfig)
            .build();

        RestHighLevelClient client = highLevelClient();
        client.transform().putTransform(new PutTransformRequest(putTransformConfig), RequestOptions.DEFAULT);
        transformsToClean.add(putTransformConfig.getId());

        {
            // tag::get-transform-request
            GetTransformRequest request =
                    new GetTransformRequest("mega-transform"); // <1>
            // end::get-transform-request

            // tag::get-transform-request-options
            request.setPageParams(new PageParams(0, 100)); // <1>
            request.setAllowNoMatch(true); // <2>
            request.setExcludeGenerated(false); // <3>
            // end::get-transform-request-options

            // tag::get-transform-execute
            GetTransformResponse response =
                client.transform()
                    .getTransform(request, RequestOptions.DEFAULT);
            // end::get-transform-execute

            // tag::get-transform-response
            List<TransformConfig> transformConfigs =
                    response.getTransformConfigurations();
            // end::get-transform-response

            assertEquals(1, transformConfigs.size());
        }
        {
            // tag::get-transform-execute-listener
            ActionListener<GetTransformResponse> listener =
                new ActionListener<GetTransformResponse>() {
                    @Override
                    public void onResponse(GetTransformResponse response) {
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

            GetTransformRequest request = new GetTransformRequest("mega-transform");

            // tag::get-transform-execute-async
            client.transform().getTransformAsync(
                    request, RequestOptions.DEFAULT, listener);  // <1>
            // end::get-transform-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
