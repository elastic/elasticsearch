/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MLTransportClientIT extends ESXPackSmokeClientTestCase {

    public void testMLTransportClient() {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        MachineLearningClient mlClient = xPackClient.machineLearning();
        Job.Builder job = new Job.Builder();
        job.setId("test");

        List<Detector> detectors = new ArrayList<>();
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detectors.add(detector.build());

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setBatchSpan(TimeValue.timeValueMinutes(5));
        job.setAnalysisConfig(analysisConfig);

        PutJobAction.Response putJobResponse = mlClient
                .putJob(new PutJobAction.Request(job))
                .actionGet();

        assertThat(putJobResponse, notNullValue());
        assertThat(putJobResponse.isAcknowledged(), equalTo(true));

        GetJobsAction.Response getJobResponse = mlClient.getJobs(new GetJobsAction.Request("test"))
                .actionGet();

        assertThat(getJobResponse, notNullValue());
        assertThat(getJobResponse.getResponse(), notNullValue());
        assertThat(getJobResponse.getResponse().count(), equalTo(1L));

        DeleteJobAction.Response deleteJobResponse = mlClient
                .deleteJob(new DeleteJobAction.Request("test"))
                .actionGet();

        assertThat(deleteJobResponse, notNullValue());
        assertThat(deleteJobResponse.isAcknowledged(), equalTo(true));
    }
}
