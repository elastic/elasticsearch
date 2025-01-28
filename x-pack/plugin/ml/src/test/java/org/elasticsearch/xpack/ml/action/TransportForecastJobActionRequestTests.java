/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TransportForecastJobActionRequestTests extends ESTestCase {

    public void testValidate_jobVersionCannonBeBefore61() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        jobBuilder.setJobVersion(MlConfigVersion.fromString("6.0.1"));
        ForecastJobAction.Request request = new ForecastJobAction.Request();
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportForecastJobAction.validate(jobBuilder.build(), request)
        );
        assertEquals("Cannot run forecast because jobs created prior to version 6.1 are not supported", e.getMessage());
    }

    public void testValidate_jobVersionCannonBeBefore61NoJobVersion() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        ForecastJobAction.Request request = new ForecastJobAction.Request();
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportForecastJobAction.validate(jobBuilder.build(), request)
        );
        assertEquals("Cannot run forecast because jobs created prior to version 6.1 are not supported", e.getMessage());
    }

    public void testValidate_DurationCannotBeLessThanBucketSpan() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        ForecastJobAction.Request request = new ForecastJobAction.Request();
        request.setDuration(TimeValue.timeValueMinutes(1));
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportForecastJobAction.validate(jobBuilder.build(new Date()), request)
        );
        assertEquals("[duration] must be greater or equal to the bucket span: [1m/1h]", e.getMessage());
    }

    public void testAdjustLimit() {
        Job.Builder jobBuilder = createTestJob("forecast-adjust-limit");
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        {
            assertThat(TransportForecastJobAction.getAdjustedMemoryLimit(jobBuilder.build(), null, auditor), is(nullValue()));
            assertThat(
                TransportForecastJobAction.getAdjustedMemoryLimit(jobBuilder.build(), ByteSizeValue.ofMb(20).getBytes(), auditor),
                equalTo(ByteSizeValue.ofMb(20).getBytes())
            );
            assertThat(
                TransportForecastJobAction.getAdjustedMemoryLimit(jobBuilder.build(), ByteSizeValue.ofMb(499).getBytes(), auditor),
                equalTo(ByteSizeValue.ofMb(499).getBytes())
            );
        }

        {
            long limit = ByteSizeValue.ofMb(100).getBytes();
            assertThat(
                TransportForecastJobAction.getAdjustedMemoryLimit(
                    jobBuilder.setAnalysisLimits(new AnalysisLimits(1L)).build(),
                    limit,
                    auditor
                ),
                equalTo(104857600L)
            );
        }

        {
            long limit = 429496732L;
            assertThat(
                TransportForecastJobAction.getAdjustedMemoryLimit(
                    jobBuilder.setAnalysisLimits(new AnalysisLimits(1L)).build(),
                    limit,
                    auditor
                ),
                equalTo(429496728L)
            );
        }

        {
            long limit = ByteSizeValue.ofMb(200).getBytes();
            assertThat(TransportForecastJobAction.getAdjustedMemoryLimit(jobBuilder.build(), limit, auditor), equalTo(limit));
            // gets adjusted down due to job analysis limits
            assertThat(
                TransportForecastJobAction.getAdjustedMemoryLimit(
                    jobBuilder.setAnalysisLimits(new AnalysisLimits(200L, null)).build(),
                    limit,
                    auditor
                ),
                equalTo(ByteSizeValue.ofMb(80).getBytes() - 1L)
            );
        }
    }

    private Job.Builder createTestJob(String jobId) {
        Job.Builder jobBuilder = new Job.Builder(jobId);
        jobBuilder.setCreateTime(new Date());
        Detector.Builder detector = new Detector.Builder("mean", "value");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        return jobBuilder;
    }
}
