/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.aggs.inference.InferencePipelineAggregationBuilder;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestPipelineTestUtils.jsonSimulatePipelineRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MachineLearningLicensingIT extends BaseMlIntegTestCase {

    public static final Set<String> RELATED_TASKS = Set.of(MlTasks.DATAFEED_TASK_NAME, MlTasks.JOB_TASK_NAME);

    @Before
    public void resetLicensing() {
        enableLicensing();

        ensureStableCluster(1);
        ensureYellow();
    }

    public void testMachineLearningPutJobActionRestricted() {
        String jobId = "testmachinelearningputjobactionrestricted";
        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
            client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), listener);
            listener.actionGet();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), listener);
        PutJobAction.Response response = listener.actionGet();
        assertNotNull(response);
    }

    public void testMachineLearningOpenJobActionRestricted() throws Exception {
        String jobId = "testmachinelearningopenjobactionrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response response = putJobListener.actionGet();
        assertNotNull(response);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<NodeAcknowledgedResponse> listener = new PlainActionFuture<>();
            client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), listener);
            listener.actionGet();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);

        // now that the license is invalid, the job should get closed:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);
        });

        // test that license restricted apis do now work
        PlainActionFuture<NodeAcknowledgedResponse> listener = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), listener);
        NodeAcknowledgedResponse response2 = listener.actionGet();
        assertNotNull(response2);
    }

    public void testMachineLearningPutDatafeedActionRestricted() {
        String jobId = "testmachinelearningputdatafeedactionrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<PutDatafeedAction.Response> listener = new PlainActionFuture<>();
            client().execute(
                PutDatafeedAction.INSTANCE,
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(jobId))),
                listener
            );
            listener.actionGet();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutDatafeedAction.Response> listener = new PlainActionFuture<>();
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(jobId))),
            listener
        );
        PutDatafeedAction.Response response = listener.actionGet();
        assertNotNull(response);
    }

    public void testAutoCloseJobWithDatafeed() throws Exception {
        String jobId = "testautoclosejobwithdatafeed";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).setMapping("""
            {"_doc":{"properties":{"time":{"type":"date"}}}}""").get();

        // put job
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        // put datafeed
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainActionFuture<>();
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(datafeedIndex))),
            putDatafeedListener
        );
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        // open job
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        NodeAcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);
        // start datafeed
        PlainActionFuture<NodeAcknowledgedResponse> listener = new PlainActionFuture<>();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener);
        listener.actionGet();

        if (randomBoolean()) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            disableLicensing();
        }
        assertMLAllowed(false);

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        // now that the license is invalid, the job should be closed and datafeed stopped:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);

            DatafeedState datafeedState = getDatafeedStats(datafeedId).getDatafeedState();
            assertEquals(DatafeedState.STOPPED, datafeedState);

            ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            List<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = findTasks(state, RELATED_TASKS);
            assertEquals(0, tasks.size());
        });

        enableLicensing(randomValidLicenseType());
        assertMLAllowed(true);

        // open job
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener2 = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener2);
        NodeAcknowledgedResponse openJobResponse3 = openJobListener2.actionGet();
        assertNotNull(openJobResponse3);
        // start datafeed
        PlainActionFuture<NodeAcknowledgedResponse> listener2 = new PlainActionFuture<>();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener2);
        listener2.actionGet();

        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.OPENED, jobState);

            DatafeedState datafeedState = getDatafeedStats(datafeedId).getDatafeedState();
            assertEquals(DatafeedState.STARTED, datafeedState);

            ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            List<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = findTasks(state, RELATED_TASKS);
            assertEquals(2, tasks.size());
        });

        if (randomBoolean()) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            disableLicensing();
        }
        assertMLAllowed(false);

        // now that the license is invalid, the job should be closed and datafeed stopped:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);

            DatafeedState datafeedState = getDatafeedStats(datafeedId).getDatafeedState();
            assertEquals(DatafeedState.STOPPED, datafeedState);

            ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            List<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = findTasks(state, RELATED_TASKS);
            assertEquals(0, tasks.size());
        });
    }

    public void testMachineLearningStartDatafeedActionRestricted() throws Exception {
        String jobId = "testmachinelearningstartdatafeedactionrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).setMapping("""
            {"_doc":{"properties":{"time":{"type":"date"}}}}""").get();
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainActionFuture<>();
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(datafeedIndex))),
            putDatafeedListener
        );
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        NodeAcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // now that the license is invalid, the job should get closed:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);
            ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            List<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = findTasks(state, RELATED_TASKS);
            assertEquals(0, tasks.size());
        });

        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<NodeAcknowledgedResponse> listener = new PlainActionFuture<>();
            client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener);
            listener.actionGet();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        // re-open job now that the license is valid again
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener2 = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener2);
        NodeAcknowledgedResponse openJobResponse3 = openJobListener2.actionGet();
        assertNotNull(openJobResponse3);

        PlainActionFuture<NodeAcknowledgedResponse> listener = new PlainActionFuture<>();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener);
        NodeAcknowledgedResponse response = listener.actionGet();
        assertNotNull(response);
    }

    public void testMachineLearningStopDatafeedActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningstopdatafeedactionnotrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).setMapping("""
            {"_doc":{"properties":{"time":{"type":"date"}}}}""").get();
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainActionFuture<>();
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(datafeedIndex))),
            putDatafeedListener
        );
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        NodeAcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);
        PlainActionFuture<NodeAcknowledgedResponse> startDatafeedListener = new PlainActionFuture<>();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), startDatafeedListener);
        NodeAcknowledgedResponse startDatafeedResponse = startDatafeedListener.actionGet();
        assertNotNull(startDatafeedResponse);

        boolean invalidLicense = randomBoolean();
        if (invalidLicense) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            enableLicensing(randomValidLicenseType());
        }

        PlainActionFuture<StopDatafeedAction.Response> listener = new PlainActionFuture<>();
        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId), listener);
        if (invalidLicense) {
            // the stop datafeed due to invalid license happens async, so check if the datafeed turns into stopped state:
            assertBusy(() -> {
                GetDatafeedsStatsAction.Response response = client().execute(
                    GetDatafeedsStatsAction.INSTANCE,
                    new GetDatafeedsStatsAction.Request(datafeedId)
                ).actionGet();
                assertEquals(DatafeedState.STOPPED, response.getResponse().results().get(0).getDatafeedState());
            });
        } else {
            listener.actionGet();
        }

        if (invalidLicense) {
            // the close due to invalid license happens async, so check if the job turns into closed state:
            assertBusy(() -> {
                GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                    .actionGet();
                assertEquals(JobState.CLOSED, response.getResponse().results().get(0).getState());
            });
        }
    }

    public void testMachineLearningCloseJobActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningclosejobactionnotrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<NodeAcknowledgedResponse> openJobListener = new PlainActionFuture<>();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        NodeAcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);

        boolean invalidLicense = randomBoolean();
        if (invalidLicense) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            enableLicensing(randomValidLicenseType());
        }

        PlainActionFuture<CloseJobAction.Response> listener = new PlainActionFuture<>();
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
        request.setCloseTimeout(TimeValue.timeValueSeconds(20));
        if (invalidLicense) {
            // the close due to invalid license happens async, so check if the job turns into closed state:
            assertBusy(() -> {
                GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                    .actionGet();
                assertEquals(JobState.CLOSED, response.getResponse().results().get(0).getState());
            });
        } else {
            client().execute(CloseJobAction.INSTANCE, request, listener);
            listener.actionGet();
        }
    }

    public void testMachineLearningDeleteJobActionNotRestricted() {
        String jobId = "testmachinelearningclosejobactionnotrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        client().execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId), listener);
        listener.actionGet();
    }

    public void testMachineLearningDeleteDatafeedActionNotRestricted() {
        String jobId = "testmachinelearningdeletedatafeedactionnotrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = new PlainActionFuture<>();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = new PlainActionFuture<>();
        client().execute(
            PutDatafeedAction.INSTANCE,
            new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(jobId))),
            putDatafeedListener
        );
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        client().execute(DeleteDatafeedAction.INSTANCE, new DeleteDatafeedAction.Request(datafeedId), listener);
        listener.actionGet();
    }

    public void testMachineLearningCreateInferenceProcessorRestricted() {
        String modelId = "modelprocessorlicensetest";
        assertMLAllowed(true);
        putInferenceModel(modelId);

        String pipeline = """
            {    "processors": [
                  {
                    "inference": {
                      "target_field": "regression_value",
                      "model_id": "modelprocessorlicensetest",
                      "inference_config": {"regression": {}},
                      "field_map": {}
                    }
                  }]}
            """;
        // Creating a pipeline should work
        putJsonPipeline("test_infer_license_pipeline", pipeline);

        prepareIndex("infer_license_test").setPipeline("test_infer_license_pipeline").setSource("{}", XContentType.JSON).get();

        String simulateSource = Strings.format("""
            {
              "pipeline": %s,
              "docs": [
                {"_source": {
                  "col1": "female",
                  "col2": "M",
                  "col3": "none",
                  "col4": 10
                }}]
            }""", pipeline);
        PlainActionFuture<SimulatePipelineResponse> simulatePipelineListener = new PlainActionFuture<>();
        client().execute(SimulatePipelineAction.INSTANCE, jsonSimulatePipelineRequest(simulateSource), simulatePipelineListener);

        assertThat(simulatePipelineListener.actionGet().getResults(), is(not(empty())));

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // Inference against the previous pipeline should still work
        try {
            prepareIndex("infer_license_test").setPipeline("test_infer_license_pipeline").setSource("{}", XContentType.JSON).get();
        } catch (ElasticsearchSecurityException ex) {
            fail(ex.getMessage());
        }

        // Creating a new pipeline with an inference processor should work
        putJsonPipeline("test_infer_license_pipeline_again", pipeline);

        // Inference against the new pipeline should fail since it has never previously succeeded
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            prepareIndex("infer_license_test").setPipeline("test_infer_license_pipeline_again").setSource("{}", XContentType.JSON).get();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Simulating the pipeline should fail
        SimulateDocumentBaseResult simulateResponse = (SimulateDocumentBaseResult) client().execute(
            SimulatePipelineAction.INSTANCE,
            jsonSimulatePipelineRequest(simulateSource)
        ).actionGet().getResults().get(0);
        assertThat(simulateResponse.getFailure(), is(not(nullValue())));
        assertThat((simulateResponse.getFailure()).getCause(), is(instanceOf(ElasticsearchSecurityException.class)));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);
        // test that license restricted apis do now work
        putJsonPipeline("test_infer_license_pipeline", pipeline);

        PlainActionFuture<SimulatePipelineResponse> simulatePipelineListenerNewLicense = new PlainActionFuture<>();
        client().execute(SimulatePipelineAction.INSTANCE, jsonSimulatePipelineRequest(simulateSource), simulatePipelineListenerNewLicense);

        assertThat(simulatePipelineListenerNewLicense.actionGet().getResults(), is(not(empty())));

        // both ingest pipelines should work

        prepareIndex("infer_license_test").setPipeline("test_infer_license_pipeline").setSource("{}", XContentType.JSON).get();
        prepareIndex("infer_license_test").setPipeline("test_infer_license_pipeline_again").setSource("{}", XContentType.JSON).get();
    }

    public void testMachineLearningInferModelRestricted() {
        String modelId = "modelinfermodellicensetest";
        assertMLAllowed(true);
        putInferenceModel(modelId);

        PlainActionFuture<InferModelAction.Response> inferModelSuccess = new PlainActionFuture<>();
        client().execute(
            InferModelAction.INSTANCE,
            InferModelAction.Request.forIngestDocs(
                modelId,
                Collections.singletonList(Collections.emptyMap()),
                RegressionConfigUpdate.EMPTY_PARAMS,
                false,
                TimeValue.timeValueSeconds(5)
            ),
            inferModelSuccess
        );
        InferModelAction.Response response = inferModelSuccess.actionGet();
        assertThat(response.getInferenceResults(), is(not(empty())));
        assertThat(response.isLicensed(), is(true));

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // inferring against a model should now fail
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            client().execute(
                InferModelAction.INSTANCE,
                InferModelAction.Request.forIngestDocs(
                    modelId,
                    Collections.singletonList(Collections.emptyMap()),
                    RegressionConfigUpdate.EMPTY_PARAMS,
                    false,
                    TimeValue.timeValueSeconds(5)
                )
            ).actionGet();
        });
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("non-compliant"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));

        // Inferring with previously Licensed == true should pass, but indicate license issues
        inferModelSuccess = new PlainActionFuture<>();
        client().execute(
            InferModelAction.INSTANCE,
            InferModelAction.Request.forIngestDocs(
                modelId,
                Collections.singletonList(Collections.emptyMap()),
                RegressionConfigUpdate.EMPTY_PARAMS,
                true,
                TimeValue.timeValueSeconds(5)
            ),
            inferModelSuccess
        );
        response = inferModelSuccess.actionGet();
        assertThat(response.getInferenceResults(), is(not(empty())));
        assertThat(response.isLicensed(), is(false));

        // Pick a license that does allow machine learning
        mode = randomValidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(true);

        PlainActionFuture<InferModelAction.Response> listener = new PlainActionFuture<>();
        client().execute(
            InferModelAction.INSTANCE,
            InferModelAction.Request.forIngestDocs(
                modelId,
                Collections.singletonList(Collections.emptyMap()),
                RegressionConfigUpdate.EMPTY_PARAMS,
                false,
                TimeValue.timeValueSeconds(5)
            ),
            listener
        );
        assertThat(listener.actionGet().getInferenceResults(), is(not(empty())));
    }

    public void testInferenceAggRestricted() {
        String modelId = "inference-agg-restricted";
        assertMLAllowed(true);
        putInferenceModel(modelId);

        // index some data
        String index = "inference-agg-licence-test";
        client().admin().indices().prepareCreate(index).setMapping("feature1", "type=double", "feature2", "type=keyword").get();
        client().prepareBulk(index)
            .add(new IndexRequest().source("feature1", "10.0", "feature2", "foo"))
            .add(new IndexRequest().source("feature1", "20.0", "feature2", "foo"))
            .add(new IndexRequest().source("feature1", "20.0", "feature2", "bar"))
            .add(new IndexRequest().source("feature1", "20.0", "feature2", "bar"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("foobar").field("feature2");
        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_feature1").field("feature1");
        termsAgg.subAggregation(avgAgg);

        XPackLicenseState licenseState = internalCluster().getInstance(XPackLicenseState.class);
        Settings settings = internalCluster().getInstance(Settings.class);
        ModelLoadingService modelLoading = internalCluster().getInstance(ModelLoadingService.class);

        Map<String, String> bucketPaths = new HashMap<>();
        bucketPaths.put("feature1", "avg_feature1");
        InferencePipelineAggregationBuilder inferenceAgg = new InferencePipelineAggregationBuilder(
            "infer_agg",
            new SetOnce<>(modelLoading),
            licenseState,
            settings,
            bucketPaths
        );
        inferenceAgg.setModelId(modelId);

        termsAgg.subAggregation(inferenceAgg);

        SearchRequest search = new SearchRequest(index);
        search.source().aggregation(termsAgg);
        client().search(search).actionGet().decRef();

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // inferring against a model should now fail
        SearchRequest invalidSearch = new SearchRequest(index);
        invalidSearch.source().aggregation(termsAgg);
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().search(invalidSearch).actionGet()
        );

        assertThat(e.status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getMessage(), containsString("current license is non-compliant for [ml]"));
        assertThat(e.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasItem(XPackField.MACHINE_LEARNING));
    }

    private void putInferenceModel(String modelId) {
        TrainedModelConfig config = TrainedModelConfig.builder()
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setTrainedModel(
                    Tree.builder()
                        .setTargetType(TargetType.REGRESSION)
                        .setFeatureNames(Collections.singletonList("feature1"))
                        .setNodes(TreeNode.builder(0).setLeafValue(1.0))
                        .build()
                ).setPreProcessors(Collections.emptyList())
            )
            .setModelId(modelId)
            .setDescription("test model for classification")
            .setInput(new TrainedModelInput(Collections.singletonList("feature1")))
            .setInferenceConfig(RegressionConfig.EMPTY_PARAMS)
            .build();
        client().execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(config, false)).actionGet();
    }

    private static OperationMode randomInvalidLicenseType() {
        return randomFrom(License.OperationMode.GOLD, License.OperationMode.STANDARD, License.OperationMode.BASIC);
    }

    private static OperationMode randomValidLicenseType() {
        return randomFrom(License.OperationMode.TRIAL, License.OperationMode.PLATINUM, OperationMode.ENTERPRISE);
    }

    private static OperationMode randomLicenseType() {
        return randomFrom(License.OperationMode.values());
    }

    private static void assertMLAllowed(boolean expected) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            assertEquals(MachineLearningField.ML_API_FEATURE.check(licenseState), expected);
        }
    }

    public static void disableLicensing() {
        disableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, false, null));
        }
    }

    public static void enableLicensing() {
        enableLicensing(randomValidLicenseType());
    }

    public static void enableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        }
    }
}
