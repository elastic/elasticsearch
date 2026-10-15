/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.EsqlDatafeedSourceCheckpoint;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedContextProviderTests extends ESTestCase {

    public void testValidateLoadedCheckpointShouldDiscardFingerprintMismatch() {
        Job job = buildJob("job-1", "time");
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", "FROM logs");
        EsqlDatafeedSourceCheckpoint checkpoint = new EsqlDatafeedSourceCheckpoint("job-1", "datafeed-1", 1000L, "stale-fingerprint");

        assertThat(DatafeedContextProvider.validateLoadedCheckpoint(datafeed, job, checkpoint), nullValue());
    }

    public void testValidateLoadedCheckpointShouldKeepMatchingFingerprint() {
        Job job = buildJob("job-1", "time");
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", "FROM logs");
        String fingerprint = EsqlDatafeedSourceCheckpoint.computeFingerprint(datafeed, "time");
        EsqlDatafeedSourceCheckpoint checkpoint = new EsqlDatafeedSourceCheckpoint("job-1", "datafeed-1", 1000L, fingerprint);

        assertThat(DatafeedContextProvider.validateLoadedCheckpoint(datafeed, job, checkpoint), equalTo(checkpoint));
    }

    private static Job buildJob(String jobId, String timeField) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        Job.Builder builder = new Job.Builder(jobId);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(new DataDescription.Builder().setTimeField(timeField));
        return builder.build(new Date());
    }

    private static DatafeedConfig buildDatafeed(String datafeedId, String jobId, String esqlQuery) {
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery(esqlQuery)
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
    }
}
