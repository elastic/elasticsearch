/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class MachineLearningLicensingTests extends BaseMlIntegTestCase {

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
            PlainActionFuture<PutJobAction.Response> listener = PlainActionFuture.newFuture();
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
        PlainActionFuture<PutJobAction.Response> listener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), listener);
        PutJobAction.Response response = listener.actionGet();
        assertNotNull(response);
    }

    public void testMachineLearningOpenJobActionRestricted() throws Exception {
        String jobId = "testmachinelearningopenjobactionrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response response = putJobListener.actionGet();
        assertNotNull(response);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
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
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), listener);
        AcknowledgedResponse response2 = listener.actionGet();
        assertNotNull(response2);
    }

    public void testMachineLearningPutDatafeedActionRestricted() throws Exception {
        String jobId = "testmachinelearningputdatafeedactionrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);
        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<PutDatafeedAction.Response> listener = PlainActionFuture.newFuture();
            client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(jobId))), listener);
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
        PlainActionFuture<PutDatafeedAction.Response> listener = PlainActionFuture.newFuture();
        client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId, Collections.singletonList(jobId))), listener);
        PutDatafeedAction.Response response = listener.actionGet();
        assertNotNull(response);
    }

    public void testAutoCloseJobWithDatafeed() throws Exception {
        String jobId = "testautoclosejobwithdatafeed";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).addMapping("type", "{\"type\":{\"properties\":{\"time\":{\"type\":\"date\"}}}}",
            XContentType.JSON).get();

        // put job
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        // put datafeed
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = PlainActionFuture.newFuture();
        client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId,
                        Collections.singletonList(datafeedIndex))), putDatafeedListener);
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        // open job
        PlainActionFuture<AcknowledgedResponse> openJobListener = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        AcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);
        // start datafeed
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener);
        listener.actionGet();


        if (randomBoolean()) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            disableLicensing();
        }
        assertMLAllowed(false);

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        // now that the license is invalid, the job should be closed and datafeed stopped:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);

            DatafeedState datafeedState = getDatafeedStats(datafeedId).getDatafeedState();
            assertEquals(DatafeedState.STOPPED, datafeedState);

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertEquals(0, tasks.taskMap().size());
        });

        enableLicensing(randomValidLicenseType());
        assertMLAllowed(true);

        // open job
        PlainActionFuture<AcknowledgedResponse> openJobListener2 = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener2);
        AcknowledgedResponse openJobResponse3 = openJobListener2.actionGet();
        assertNotNull(openJobResponse3);
        // start datafeed
        PlainActionFuture<AcknowledgedResponse> listener2 = PlainActionFuture.newFuture();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener2);
        listener2.actionGet();

        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.OPENED, jobState);

            DatafeedState datafeedState = getDatafeedStats(datafeedId).getDatafeedState();
            assertEquals(DatafeedState.STARTED, datafeedState);

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertEquals(2, tasks.taskMap().size());
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

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertEquals(0, tasks.taskMap().size());
        });
    }

    public void testMachineLearningStartDatafeedActionRestricted() throws Exception {
        String jobId = "testmachinelearningstartdatafeedactionrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).addMapping("type", "{\"type\":{\"properties\":{\"time\":{\"type\":\"date\"}}}}",
            XContentType.JSON).get();
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = PlainActionFuture.newFuture();
        client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId,
                        Collections.singletonList(datafeedIndex))), putDatafeedListener);
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        PlainActionFuture<AcknowledgedResponse> openJobListener = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        AcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);

        // Pick a license that does not allow machine learning
        License.OperationMode mode = randomInvalidLicenseType();
        enableLicensing(mode);
        assertMLAllowed(false);

        // now that the license is invalid, the job should get closed:
        assertBusy(() -> {
            JobState jobState = getJobStats(jobId).getState();
            assertEquals(JobState.CLOSED, jobState);
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertEquals(0, tasks.taskMap().size());
        });

        // test that license restricted apis do not work
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
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
        PlainActionFuture<AcknowledgedResponse> openJobListener2 = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener2);
        AcknowledgedResponse openJobResponse3 = openJobListener2.actionGet();
        assertNotNull(openJobResponse3);

        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L), listener);
        AcknowledgedResponse response = listener.actionGet();
        assertNotNull(response);
    }

    public void testMachineLearningStopDatafeedActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningstopdatafeedactionnotrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        String datafeedIndex = jobId + "-data";
        prepareCreate(datafeedIndex).addMapping("type", "{\"type\":{\"properties\":{\"time\":{\"type\":\"date\"}}}}",
            XContentType.JSON).get();
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = PlainActionFuture.newFuture();
        client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId,
                        Collections.singletonList(datafeedIndex))), putDatafeedListener);
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);
        PlainActionFuture<AcknowledgedResponse> openJobListener = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        AcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);
        PlainActionFuture<AcknowledgedResponse> startDatafeedListener = PlainActionFuture.newFuture();
        client().execute(StartDatafeedAction.INSTANCE, 
            new StartDatafeedAction.Request(datafeedId, 0L), startDatafeedListener);
        AcknowledgedResponse startDatafeedResponse = startDatafeedListener.actionGet();
        assertNotNull(startDatafeedResponse);

        boolean invalidLicense = randomBoolean();
        if (invalidLicense) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            enableLicensing(randomValidLicenseType());
        }

        PlainActionFuture<StopDatafeedAction.Response> listener = PlainActionFuture.newFuture();
        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId), listener);
        if (invalidLicense) {
            // the stop datafeed due to invalid license happens async, so check if the datafeed turns into stopped state:
            assertBusy(() -> {
                GetDatafeedsStatsAction.Response response =
                        client().execute(GetDatafeedsStatsAction.INSTANCE, new GetDatafeedsStatsAction.Request(datafeedId)).actionGet();
                assertEquals(DatafeedState.STOPPED, response.getResponse().results().get(0).getDatafeedState());
            });
        } else {
            listener.actionGet();
        }

        if (invalidLicense) {
            // the close due to invalid license happens async, so check if the job turns into closed state:
            assertBusy(() -> {
                GetJobsStatsAction.Response response =
                        client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId)).actionGet();
                assertEquals(JobState.CLOSED, response.getResponse().results().get(0).getState());
            });
        }
    }

    public void testMachineLearningCloseJobActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningclosejobactionnotrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<AcknowledgedResponse> openJobListener = PlainActionFuture.newFuture();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId), openJobListener);
        AcknowledgedResponse openJobResponse = openJobListener.actionGet();
        assertNotNull(openJobResponse);

        boolean invalidLicense = randomBoolean();
        if (invalidLicense) {
            enableLicensing(randomInvalidLicenseType());
        } else {
            enableLicensing(randomValidLicenseType());
        }

        PlainActionFuture<CloseJobAction.Response> listener = PlainActionFuture.newFuture();
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
        request.setCloseTimeout(TimeValue.timeValueSeconds(20));
        if (invalidLicense) {
            // the close due to invalid license happens async, so check if the job turns into closed state:
            assertBusy(() -> {
                GetJobsStatsAction.Response response =
                        client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId)).actionGet();
                assertEquals(JobState.CLOSED, response.getResponse().results().get(0).getState());
            });
        } else {
            client().execute(CloseJobAction.INSTANCE, request, listener);
            listener.actionGet();
        }
    }

    public void testMachineLearningDeleteJobActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningclosejobactionnotrestricted";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client().execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId), listener);
        listener.actionGet();
    }

    public void testMachineLearningDeleteDatafeedActionNotRestricted() throws Exception {
        String jobId = "testmachinelearningdeletedatafeedactionnotrestricted";
        String datafeedId = jobId + "-datafeed";
        assertMLAllowed(true);
        // test that license restricted apis do now work
        PlainActionFuture<PutJobAction.Response> putJobListener = PlainActionFuture.newFuture();
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(createJob(jobId)), putJobListener);
        PutJobAction.Response putJobResponse = putJobListener.actionGet();
        assertNotNull(putJobResponse);
        PlainActionFuture<PutDatafeedAction.Response> putDatafeedListener = PlainActionFuture.newFuture();
        client().execute(PutDatafeedAction.INSTANCE, 
                new PutDatafeedAction.Request(createDatafeed(datafeedId, jobId,
                        Collections.singletonList(jobId))), putDatafeedListener);
        PutDatafeedAction.Response putDatafeedResponse = putDatafeedListener.actionGet();
        assertNotNull(putDatafeedResponse);

        // Pick a random license
        License.OperationMode mode = randomLicenseType();
        enableLicensing(mode);

        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client().execute(DeleteDatafeedAction.INSTANCE, new DeleteDatafeedAction.Request(datafeedId), listener);
        listener.actionGet();
    }

    private static OperationMode randomInvalidLicenseType() {
        return randomFrom(License.OperationMode.GOLD, License.OperationMode.STANDARD, License.OperationMode.BASIC);
    }

    private static OperationMode randomValidLicenseType() {
        return randomFrom(License.OperationMode.TRIAL, License.OperationMode.PLATINUM);
    }

    private static OperationMode randomLicenseType() {
        return randomFrom(License.OperationMode.values());
    }

    private static void assertMLAllowed(boolean expected) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            assertEquals(licenseState.isMachineLearningAllowed(), expected);
        }
    }

    public static void disableLicensing() {
        disableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(operationMode, false, null);
        }
    }

    public static void enableLicensing() {
        enableLicensing(randomValidLicenseType());
    }

    public static void enableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(operationMode, true, null);
        }
    }
}
