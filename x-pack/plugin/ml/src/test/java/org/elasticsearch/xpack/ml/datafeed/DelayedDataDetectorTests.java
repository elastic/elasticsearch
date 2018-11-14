/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;


public class DelayedDataDetectorTests extends ESTestCase {


    public void testConstructorWithValueValues() {
        TimeValue window = TimeValue.timeValueSeconds(10);
        Job job = createJob(TimeValue.timeValueSeconds(1));
        DelayedDataDetector delayedDataDetector = new DelayedDataDetector(job, createDatafeed(), window, mock(Client.class));
        assertNotNull(delayedDataDetector);
    }

    public void testConstructorWithInvalidValues() {
        TimeValue shortWindow = TimeValue.timeValueMillis(500);
        Job job = createJob(TimeValue.timeValueSeconds(1));

        Exception exception = expectThrows(IllegalArgumentException.class,
            ()-> new DelayedDataDetector(job, createDatafeed(), shortWindow, mock(Client.class)));
        assertThat(exception.getMessage(), equalTo("[window] must be greater or equal to the [bucket_span]"));

        TimeValue longWindow = TimeValue.timeValueSeconds(20000);

        exception = expectThrows(IllegalArgumentException.class,
            ()-> new DelayedDataDetector(job, createDatafeed(), longWindow, mock(Client.class)));
        assertThat(exception.getMessage(), equalTo("[window] must contain less than 10000 buckets at the current [bucket_span]"));
    }


    private Job createJob(TimeValue bucketSpan) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        dataDescription.setTimeField("time");
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(bucketSpan);

        Job.Builder builder = new Job.Builder();
        builder.setId("test-job");
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder.build(new Date());
    }

    private DatafeedConfig createDatafeed() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("id", "jobId");
        builder.setIndices(Collections.singletonList("index1"));
        builder.setTypes(Collections.singletonList("doc"));
        return builder.build();
    }



}
