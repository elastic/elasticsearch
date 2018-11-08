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
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;


public class DelayedDataDetectorTests extends ESTestCase {


    public void testConstructor() {
            Job job = createJob(TimeValue.timeValueMinutes(5));

            DatafeedConfig datafeedConfig = createDatafeed(false, null);

            // Should not throw
            DelayedDataDetector delayedDataDetector = new DelayedDataDetector(job, datafeedConfig, mock(Client.class));

            datafeedConfig = createDatafeed(true, TimeValue.timeValueMinutes(10));

            // Should not throw
            delayedDataDetector = new DelayedDataDetector(job, datafeedConfig, mock(Client.class));

            DatafeedConfig tooSmallDatafeedConfig = createDatafeed(true, TimeValue.timeValueMinutes(1));
            IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> new DelayedDataDetector(job, tooSmallDatafeedConfig, mock(Client.class)));
            assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL), e.getMessage());

            DatafeedConfig tooBigDatafeedConfig = createDatafeed(true, TimeValue.timeValueMinutes(5_000_000));
            e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> new DelayedDataDetector(job, tooBigDatafeedConfig, mock(Client.class)));
            assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_LARGE), e.getMessage());
    }

    public void testDetectMissingData_WhenShouldDetectDelayedDataIsFalse() {
        Job job = createJob(TimeValue.timeValueSeconds(1));
        DelayedDataDetector delayedDataDetector = new DelayedDataDetector(job, createDatafeed(false,
            TimeValue.timeValueMinutes(10)),
            mock(Client.class));

        assertThat(delayedDataDetector.detectMissingData(100000).isEmpty(), equalTo(true));
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

    private DatafeedConfig createDatafeed(boolean shouldDetectDelayedData, TimeValue delayedDatacheckWindow) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("id", "jobId");
        builder.setIndices(Collections.singletonList("index1"));
        builder.setTypes(Collections.singletonList("doc"));

        builder.setShouldRunDelayedDataCheck(shouldDetectDelayedData);
        builder.setDelayedDataCheckWindow(delayedDatacheckWindow);
        return builder.build();
    }



}
