/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.ForecastJobAction.Request;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class ForecastJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setDuration(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)).getStringRep());
        }
        if (randomBoolean()) {
            request.setExpiresIn(TimeValue.timeValueSeconds(randomIntBetween(0, 1_000_000)).getStringRep());
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    public void testSetDuration_GivenZero() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setDuration("0"));
        assertThat(e.getMessage(), equalTo("[duration] must be positive: [0ms]"));
    }

    public void testSetDuration_GivenNegative() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setDuration("-1s"));
        assertThat(e.getMessage(), equalTo("[duration] must be positive: [-1]"));
    }

    public void testSetExpiresIn_GivenZero() {
        Request request = new Request();
        request.setExpiresIn("0");
        assertThat(request.getExpiresIn(), equalTo(TimeValue.ZERO));
    }

    public void testSetExpiresIn_GivenNegative() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Request().setExpiresIn("-1s"));
        assertThat(e.getMessage(), equalTo("[expires_in] must be non-negative: [-1]"));
    }

    public void testValidate_jobVersionCannonBeBefore61() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        jobBuilder.setJobVersion(Version.V_6_0_1);
        ForecastJobAction.Request request = new ForecastJobAction.Request();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportForecastJobAction.validate(jobBuilder.build(), request));
        assertEquals(
                "Cannot run forecast because jobs created prior to version 6.1 are not supported",
                e.getMessage());
    }

    public void testValidate_jobVersionCannonBeBefore61NoJobVersion() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        ForecastJobAction.Request request = new ForecastJobAction.Request();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportForecastJobAction.validate(jobBuilder.build(), request));
        assertEquals(
                "Cannot run forecast because jobs created prior to version 6.1 are not supported",
                e.getMessage());
    }

    public void testValidate_DurationCannotBeLessThanBucketSpan() {
        Job.Builder jobBuilder = createTestJob("forecast-it-test-job-version");

        ForecastJobAction.Request request = new ForecastJobAction.Request();
        request.setDuration(TimeValue.timeValueMinutes(1));
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportForecastJobAction.validate(jobBuilder.build(new Date()), request));
        assertEquals("[duration] must be greater or equal to the bucket span: [1m/1h]", e.getMessage());
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
