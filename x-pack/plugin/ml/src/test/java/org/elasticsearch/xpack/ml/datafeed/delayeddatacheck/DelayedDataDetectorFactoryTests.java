/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;


public class DelayedDataDetectorFactoryTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testBuilder() {
        Job job = createJob(TimeValue.timeValueSeconds(2));

        DatafeedConfig datafeedConfig = createDatafeed(false, null);

        // Should not throw
        assertThat(DelayedDataDetectorFactory.buildDetector(job, datafeedConfig, mock(Client.class), xContentRegistry()),
            instanceOf(NullDelayedDataDetector.class));

        datafeedConfig = createDatafeed(true, TimeValue.timeValueMinutes(10));

        // Should not throw
        assertThat(DelayedDataDetectorFactory.buildDetector(job, datafeedConfig, mock(Client.class), xContentRegistry()),
            instanceOf(DatafeedDelayedDataDetector.class));

        DatafeedConfig tooSmallDatafeedConfig = createDatafeed(true, TimeValue.timeValueSeconds(1));
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
            () -> DelayedDataDetectorFactory.buildDetector(job, tooSmallDatafeedConfig, mock(Client.class), xContentRegistry()));
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL, "1s", "2s"), e.getMessage());

        DatafeedConfig tooBigDatafeedConfig = createDatafeed(true, TimeValue.timeValueHours(12));
        e = ESTestCase.expectThrows(IllegalArgumentException.class,
            () -> DelayedDataDetectorFactory.buildDetector(job, tooBigDatafeedConfig, mock(Client.class), xContentRegistry()));
        assertEquals(Messages.getMessage(
            Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_SPANS_TOO_MANY_BUCKETS, "12h", "2s"), e.getMessage());

        Job withBigBucketSpan = createJob(TimeValue.timeValueHours(1));
        datafeedConfig = createDatafeed(true, null);

        // Should not throw
        DelayedDataDetector delayedDataDetector =
            DelayedDataDetectorFactory.buildDetector(withBigBucketSpan, datafeedConfig, mock(Client.class), xContentRegistry());
        assertThat(delayedDataDetector.getWindow(), equalTo(TimeValue.timeValueHours(1).millis() * 8));

        datafeedConfig = createDatafeed(true, null);

        // Should not throw
        delayedDataDetector =
            DelayedDataDetectorFactory.buildDetector(job, datafeedConfig, mock(Client.class), xContentRegistry());
        assertThat(delayedDataDetector.getWindow(), equalTo(TimeValue.timeValueHours(2).millis()));

    }

    private Job createJob(TimeValue bucketSpan) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
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

        if (shouldDetectDelayedData) {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(delayedDatacheckWindow, null));
        } else {
            builder.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());
        }
        return builder.build();
    }

}
