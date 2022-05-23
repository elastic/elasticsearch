/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.client;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.ESXPackSmokeClientTestCase;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.client.MachineLearningClient;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MLTransportClientIT extends ESXPackSmokeClientTestCase {

    public void testMLTransportClient_JobActions() {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        MachineLearningClient mlClient = xPackClient.machineLearning();

        String jobId = "ml-transport-client-it-job";
        Job.Builder job = createJob(jobId);

        PutJobAction.Response putJobResponse = mlClient.putJob(new PutJobAction.Request(job)).actionGet();
        assertThat(putJobResponse, notNullValue());

        GetJobsAction.Response getJobResponse = mlClient.getJobs(new GetJobsAction.Request(jobId)).actionGet();
        assertThat(getJobResponse, notNullValue());
        assertThat(getJobResponse.getResponse(), notNullValue());
        assertThat(getJobResponse.getResponse().count(), equalTo(1L));

        // Open job POST data, flush, close and check a result
        AcknowledgedResponse openJobResponse = mlClient.openJob(new OpenJobAction.Request(jobId)).actionGet();
        assertThat(openJobResponse.isAcknowledged(), equalTo(true));

        String content = "{\"time\":1000, \"msg\": \"some categorical message\"}\n"
            + "{\"time\":11000, \"msg\": \"some categorical message in the second bucket\"}\n"
            + "{\"time\":21000, \"msg\": \"some categorical message in the third bucket\"}\n";
        PostDataAction.Request postRequest = new PostDataAction.Request(jobId);
        postRequest.setContent(new BytesArray(content), XContentType.JSON);
        PostDataAction.Response postResponse = mlClient.postData(postRequest).actionGet();
        assertThat(postResponse.getDataCounts(), notNullValue());
        assertThat(postResponse.getDataCounts().getInputFieldCount(), equalTo(3L));

        FlushJobAction.Response flushResponse = mlClient.flushJob(new FlushJobAction.Request(jobId)).actionGet();
        assertThat(flushResponse.isFlushed(), equalTo(true));

        CloseJobAction.Response closeResponse = mlClient.closeJob(new CloseJobAction.Request(jobId)).actionGet();
        assertThat(closeResponse.isClosed(), equalTo(true));

        GetBucketsAction.Response getBucketsResponse = mlClient.getBuckets(new GetBucketsAction.Request(jobId)).actionGet();
        assertThat(getBucketsResponse.getBuckets().count(), equalTo(1L));

        // Update a model snapshot
        GetModelSnapshotsAction.Response getModelSnapshotResponse = mlClient.getModelSnapshots(
            new GetModelSnapshotsAction.Request(jobId, null)
        ).actionGet();
        assertThat(getModelSnapshotResponse.getPage().count(), equalTo(1L));
        String snapshotId = getModelSnapshotResponse.getPage().results().get(0).getSnapshotId();

        UpdateModelSnapshotAction.Request updateModelSnapshotRequest = new UpdateModelSnapshotAction.Request(jobId, snapshotId);
        updateModelSnapshotRequest.setDescription("Changed description");
        UpdateModelSnapshotAction.Response updateModelSnapshotResponse = mlClient.updateModelSnapshot(updateModelSnapshotRequest)
            .actionGet();
        assertThat(updateModelSnapshotResponse.getModel(), notNullValue());
        assertThat(updateModelSnapshotResponse.getModel().getDescription(), equalTo("Changed description"));

        // and delete the job
        AcknowledgedResponse deleteJobResponse = mlClient.deleteJob(new DeleteJobAction.Request(jobId)).actionGet();
        assertThat(deleteJobResponse, notNullValue());
        assertThat(deleteJobResponse.isAcknowledged(), equalTo(true));
    }

    public void testMLTransportClient_ValidateActions() {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        MachineLearningClient mlClient = xPackClient.machineLearning();

        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        ValidateDetectorAction.Request validateDetectorRequest = new ValidateDetectorAction.Request(detector.build());
        AcknowledgedResponse validateDetectorResponse = mlClient.validateDetector(validateDetectorRequest).actionGet();
        assertThat(validateDetectorResponse.isAcknowledged(), equalTo(true));

        Job.Builder job = createJob("ml-transport-client-it-validate-job");
        ValidateJobConfigAction.Request validateJobRequest = new ValidateJobConfigAction.Request(job.build(new Date()));
        AcknowledgedResponse validateJobResponse = mlClient.validateJobConfig(validateJobRequest).actionGet();
        assertThat(validateJobResponse.isAcknowledged(), equalTo(true));
    }

    public void testMLTransportClient_DateFeedActions() {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        MachineLearningClient mlClient = xPackClient.machineLearning();

        String jobId = "ml-transport-client-it-datafeed-job";
        Job.Builder job = createJob(jobId);

        PutJobAction.Response putJobResponse = mlClient.putJob(new PutJobAction.Request(job)).actionGet();
        assertThat(putJobResponse, notNullValue());

        String datafeedId = "ml-transport-client-it-datafeed";
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder(datafeedId, jobId);
        String datafeedIndex = "ml-transport-client-test";
        String datatype = "type-bar";
        datafeed.setIndices(Collections.singletonList(datafeedIndex));

        mlClient.putDatafeed(new PutDatafeedAction.Request(datafeed.build())).actionGet();

        GetDatafeedsAction.Response getDatafeedResponse = mlClient.getDatafeeds(new GetDatafeedsAction.Request(datafeedId)).actionGet();
        assertThat(getDatafeedResponse.getResponse(), notNullValue());

        // Open job before starting the datafeed
        AcknowledgedResponse openJobResponse = mlClient.openJob(new OpenJobAction.Request(jobId)).actionGet();
        assertThat(openJobResponse.isAcknowledged(), equalTo(true));

        // create the index for the data feed
        Map<String, Object> source = new HashMap<>();
        source.put("time", new Date());
        source.put("message", "some message");
        client.prepareIndex(datafeedIndex, datatype).setSource(source).get();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedId, new Date().getTime());
        AcknowledgedResponse startDataFeedResponse = mlClient.startDatafeed(startDatafeedRequest).actionGet();
        assertThat(startDataFeedResponse.isAcknowledged(), equalTo(true));

        StopDatafeedAction.Response stopDataFeedResponse = mlClient.stopDatafeed(new StopDatafeedAction.Request(datafeedId)).actionGet();
        assertThat(stopDataFeedResponse.isStopped(), equalTo(true));
    }

    private Job.Builder createJob(String jobId) {
        Job.Builder job = new Job.Builder();
        job.setId(jobId);

        List<Detector> detectors = new ArrayList<>();
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detectors.add(detector.build());

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(10L));
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder());
        return job;
    }
}
