/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.JOB_FORECAST_NATIVE_PROCESS_KILLED;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ForecastIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() {
        cleanUp();
    }

    public void testSingleSeries() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-single-series");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            data.add(createJsonRecord(createRecord(timestamp, 10.0)));
            data.add(createJsonRecord(createRecord(timestamp, 30.0)));
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);

        // Now we can start doing forecast requests

        String forecastIdDefaultDurationDefaultExpiry = forecast(job.getId(), null, null);
        String forecastIdDuration1HourNoExpiry = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        String forecastIdDuration3HoursExpiresIn24Hours = forecast(job.getId(), TimeValue.timeValueHours(3), TimeValue.timeValueHours(24));

        waitForecastToFinish(job.getId(), forecastIdDefaultDurationDefaultExpiry);
        waitForecastToFinish(job.getId(), forecastIdDuration1HourNoExpiry);
        waitForecastToFinish(job.getId(), forecastIdDuration3HoursExpiresIn24Hours);
        closeJob(job.getId());

        List<Bucket> buckets = getBuckets(job.getId());
        Bucket lastBucket = buckets.get(buckets.size() - 1);
        long lastBucketTime = lastBucket.getTimestamp().getTime();

        // Now let's verify forecasts
        double expectedForecastValue = 20.0;

        List<ForecastRequestStats> forecastStats = getForecastStats();
        assertThat(forecastStats.size(), equalTo(3));
        Map<String, ForecastRequestStats> idToForecastStats = new HashMap<>();
        forecastStats.forEach(f -> idToForecastStats.put(f.getForecastId(), f));

        {
            ForecastRequestStats forecastDefaultDurationDefaultExpiry = idToForecastStats.get(forecastIdDefaultDurationDefaultExpiry);
            assertThat(
                forecastDefaultDurationDefaultExpiry.getExpiryTime().toEpochMilli(),
                equalTo(forecastDefaultDurationDefaultExpiry.getCreateTime().toEpochMilli() + TimeValue.timeValueHours(14 * 24).getMillis())
            );
            List<Forecast> forecasts = getForecasts(job.getId(), forecastDefaultDurationDefaultExpiry);
            assertThat(forecastDefaultDurationDefaultExpiry.getRecordCount(), equalTo(24L));
            assertThat(forecasts.size(), equalTo(24));
            assertThat(forecasts.get(0).getTimestamp().getTime(), equalTo(lastBucketTime));
            for (int i = 0; i < forecasts.size(); i++) {
                Forecast forecast = forecasts.get(i);
                assertThat(forecast.getTimestamp().getTime(), equalTo(lastBucketTime + i * bucketSpan.getMillis()));
                assertThat(forecast.getBucketSpan(), equalTo(bucketSpan.getSeconds()));
                assertThat(forecast.getForecastPrediction(), closeTo(expectedForecastValue, 0.01));
            }
        }

        {
            ForecastRequestStats forecastDuration1HourNoExpiry = idToForecastStats.get(forecastIdDuration1HourNoExpiry);
            assertThat(forecastDuration1HourNoExpiry.getExpiryTime(), equalTo(Instant.EPOCH));
            List<Forecast> forecasts = getForecasts(job.getId(), forecastDuration1HourNoExpiry);
            assertThat(forecastDuration1HourNoExpiry.getRecordCount(), equalTo(1L));
            assertThat(forecasts.size(), equalTo(1));
            assertThat(forecasts.get(0).getTimestamp().getTime(), equalTo(lastBucketTime));
            for (int i = 0; i < forecasts.size(); i++) {
                Forecast forecast = forecasts.get(i);
                assertThat(forecast.getTimestamp().getTime(), equalTo(lastBucketTime + i * bucketSpan.getMillis()));
                assertThat(forecast.getBucketSpan(), equalTo(bucketSpan.getSeconds()));
                assertThat(forecast.getForecastPrediction(), closeTo(expectedForecastValue, 0.01));
            }
        }

        {
            ForecastRequestStats forecastDuration3HoursExpiresIn24Hours = idToForecastStats.get(forecastIdDuration3HoursExpiresIn24Hours);
            assertThat(
                forecastDuration3HoursExpiresIn24Hours.getExpiryTime().toEpochMilli(),
                equalTo(forecastDuration3HoursExpiresIn24Hours.getCreateTime().toEpochMilli() + TimeValue.timeValueHours(24).getMillis())
            );
            List<Forecast> forecasts = getForecasts(job.getId(), forecastDuration3HoursExpiresIn24Hours);
            assertThat(forecastDuration3HoursExpiresIn24Hours.getRecordCount(), equalTo(3L));
            assertThat(forecasts.size(), equalTo(3));
            assertThat(forecasts.get(0).getTimestamp().getTime(), equalTo(lastBucketTime));
            for (int i = 0; i < forecasts.size(); i++) {
                Forecast forecast = forecasts.get(i);
                assertThat(forecast.getTimestamp().getTime(), equalTo(lastBucketTime + i * bucketSpan.getMillis()));
                assertThat(forecast.getBucketSpan(), equalTo(bucketSpan.getSeconds()));
                assertThat(forecast.getForecastPrediction(), closeTo(expectedForecastValue, 0.01));
            }
        }
    }

    public void testDurationCannotBeLessThanBucketSpan() {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-duration-bucket-span");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> forecast(job.getId(), TimeValue.timeValueMinutes(10), null)
        );
        assertThat(e.getMessage(), equalTo("[duration] must be greater or equal to the bucket span: [10m/1h]"));
    }

    public void testNoData() {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueMinutes(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-no-data");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> forecast(job.getId(), TimeValue.timeValueMinutes(120), null)
        );
        assertThat(
            e.getMessage(),
            equalTo("Cannot run forecast: Forecast cannot be executed as job requires data to have been processed and modeled")
        );
    }

    public void testMemoryStatus() {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setByFieldName("clientIP");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-memory-status");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());
        createDataWithLotsOfClientIps(bucketSpan, job);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> forecast(job.getId(), TimeValue.timeValueMinutes(120), null)
        );
        assertThat(e.getMessage(), equalTo("Cannot run forecast: Forecast cannot be executed as model memory status is not OK"));
    }

    public void testOverflowToDisk() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setByFieldName("clientIP");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-overflow-to-disk");
        AnalysisLimits limits = new AnalysisLimits(1200L, null);
        job.setAnalysisLimits(limits);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());
        createDataWithLotsOfClientIps(bucketSpan, job);

        try {
            String forecastId = forecast(job.getId(), TimeValue.timeValueHours(1), null);

            waitForecastToFinish(job.getId(), forecastId);
        } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("disk space")) {
                throw new ElasticsearchStatusException(
                    "Test likely fails due to insufficient disk space on test machine, please free up space.",
                    e.status(),
                    e
                );
            }
            throw e;
        }

        // flushing the job forces an index refresh, see https://github.com/elastic/elasticsearch/issues/31173
        flushJob(job.getId(), false);

        List<ForecastRequestStats> forecastStats = getForecastStats();
        assertThat(forecastStats.size(), equalTo(1));
        ForecastRequestStats forecastRequestStats = forecastStats.get(0);
        List<Forecast> forecasts = getForecasts(job.getId(), forecastRequestStats);

        assertThat(forecastRequestStats.getRecordCount(), equalTo(8000L));
        assertThat(forecasts.size(), equalTo(8000));

        // run forecast a 2nd time
        try {
            String forecastId = forecast(job.getId(), TimeValue.timeValueHours(1), null);

            waitForecastToFinish(job.getId(), forecastId);
        } catch (ElasticsearchStatusException e) {
            if (e.getMessage().contains("disk space")) {
                throw new ElasticsearchStatusException(
                    "Test likely fails due to insufficient disk space on test machine, please free up space.",
                    e.status(),
                    e
                );
            }
            throw e;
        }

        closeJob(job.getId());

        forecastStats = getForecastStats();
        assertThat(forecastStats.size(), equalTo(2));
        for (ForecastRequestStats stats : forecastStats) {
            forecasts = getForecasts(job.getId(), stats);

            assertThat(forecastRequestStats.getRecordCount(), equalTo(8000L));
            assertThat(forecasts.size(), equalTo(8000));
        }

    }

    public void testDeleteWildCard() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");

        Job.Builder job = new Job.Builder("forecast-it-test-delete-wildcard");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            data.add(createJsonRecord(createRecord(timestamp, 10.0)));
            data.add(createJsonRecord(createRecord(timestamp, 30.0)));
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);
        String forecastIdDefaultDurationDefaultExpiry = forecast(job.getId(), null, null);
        String forecastIdDuration1HourNoExpiry = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        String forecastId2Duration1HourNoExpiry = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        String forecastId2Duration1HourNoExpiry2 = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        waitForecastToFinish(job.getId(), forecastIdDefaultDurationDefaultExpiry);
        waitForecastToFinish(job.getId(), forecastIdDuration1HourNoExpiry);
        waitForecastToFinish(job.getId(), forecastId2Duration1HourNoExpiry);
        waitForecastToFinish(job.getId(), forecastId2Duration1HourNoExpiry2);
        closeJob(job.getId());

        assertNotNull(getForecastStats(job.getId(), forecastIdDefaultDurationDefaultExpiry));
        assertNotNull(getForecastStats(job.getId(), forecastIdDuration1HourNoExpiry));
        assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry));
        assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry2));

        {
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(
                job.getId(),
                forecastIdDefaultDurationDefaultExpiry.substring(0, forecastIdDefaultDurationDefaultExpiry.length() - 2)
                    + "*"
                    + ","
                    + forecastIdDuration1HourNoExpiry
            );
            AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertNull(getForecastStats(job.getId(), forecastIdDefaultDurationDefaultExpiry));
            assertNull(getForecastStats(job.getId(), forecastIdDuration1HourNoExpiry));
            assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry));
            assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry2));
        }

        {
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(job.getId(), "*");
            AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry));
            assertNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry2));
        }
    }

    public void testDeleteAll() throws Exception {
        Detector.Builder detector1 = new Detector.Builder("sum", "value").setPartitionFieldName("category");
        Detector.Builder detector2 = new Detector.Builder("mean", "value").setPartitionFieldName("category");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector1.build(), detector2.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");

        Job.Builder job = new Job.Builder("forecast-it-test-delete-wildcard-2");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);
        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        String[] partitionFieldValues = IntStream.range(0, 20).mapToObj(i -> "category_" + i).toArray(String[]::new);
        while (timestamp < now) {
            for (String partitionFieldValue : partitionFieldValues) {
                data.add(createJsonRecord(createRecord(timestamp, partitionFieldValue, 10.0)));
                data.add(createJsonRecord(createRecord(timestamp, partitionFieldValue, 30.0)));
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);

        long noForecasts = 11;  // We want to make sure we set the search size instead of relying on the default
        List<String> forecastIds = new ArrayList<>();
        for (int i = 0; i < noForecasts; ++i) {
            String forecastId = forecast(job.getId(), TimeValue.timeValueHours(100), TimeValue.ZERO);
            forecastIds.add(forecastId);
            waitForecastToFinish(job.getId(), forecastId);
        }
        closeJob(job.getId());

        assertThat(getJobStats(job.getId()).get(0).getForecastStats().getTotal(), is(equalTo(noForecasts)));
        for (String forecastId : forecastIds) {
            assertNotNull(getForecastStats(job.getId(), forecastId));
        }

        DeleteForecastAction.Request request = new DeleteForecastAction.Request(job.getId(), randomBoolean() ? "*" : "_all");
        AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());

        assertThat(getJobStats(job.getId()).get(0).getForecastStats().getTotal(), is(equalTo(0L)));
        for (String forecastId : forecastIds) {
            assertNull(getForecastStats(job.getId(), forecastId));
        }
    }

    public void testDelete() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");

        Job.Builder job = new Job.Builder("forecast-it-test-delete");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            data.add(createJsonRecord(createRecord(timestamp, 10.0)));
            data.add(createJsonRecord(createRecord(timestamp, 30.0)));
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);
        String forecastIdDefaultDurationDefaultExpiry = forecast(job.getId(), null, null);
        String forecastIdDuration1HourNoExpiry = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        String forecastId2Duration1HourNoExpiry = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        String forecastId2Duration1HourNoExpiry2 = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
        waitForecastToFinish(job.getId(), forecastIdDefaultDurationDefaultExpiry);
        waitForecastToFinish(job.getId(), forecastIdDuration1HourNoExpiry);
        closeJob(job.getId());

        {
            ForecastRequestStats forecastStats = getForecastStats(job.getId(), forecastIdDefaultDurationDefaultExpiry);
            assertNotNull(forecastStats);
            ForecastRequestStats otherStats = getForecastStats(job.getId(), forecastIdDuration1HourNoExpiry);
            assertNotNull(otherStats);
        }

        {
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(
                job.getId(),
                forecastIdDefaultDurationDefaultExpiry + "," + forecastIdDuration1HourNoExpiry
            );
            AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertNull(getForecastStats(job.getId(), forecastIdDefaultDurationDefaultExpiry));
            assertNull(getForecastStats(job.getId(), forecastIdDuration1HourNoExpiry));
            assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry));
            assertNotNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry2));
        }

        {
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(job.getId(), "forecast-does-not-exist");
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> client().execute(DeleteForecastAction.INSTANCE, request).actionGet()
            );
            assertThat(e.getMessage(), equalTo("No forecast(s) [forecast-does-not-exist] exists for job [forecast-it-test-delete]"));
        }

        {
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(job.getId(), Metadata.ALL);
            AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());

            assertNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry));
            assertNull(getForecastStats(job.getId(), forecastId2Duration1HourNoExpiry2));
        }

        {
            Job.Builder otherJob = new Job.Builder("forecasts-delete-with-all-and-allow-no-forecasts");
            otherJob.setAnalysisConfig(analysisConfig);
            otherJob.setDataDescription(dataDescription);

            putJob(otherJob);
            DeleteForecastAction.Request request = new DeleteForecastAction.Request(otherJob.getId(), Metadata.ALL);
            AcknowledgedResponse response = client().execute(DeleteForecastAction.INSTANCE, request).actionGet();
            assertTrue(response.isAcknowledged());
        }

        {
            Job.Builder otherJob = new Job.Builder("forecasts-delete-with-all-and-not-allow-no-forecasts");
            otherJob.setAnalysisConfig(analysisConfig);
            otherJob.setDataDescription(dataDescription);

            putJob(otherJob);

            DeleteForecastAction.Request request = new DeleteForecastAction.Request(otherJob.getId(), Metadata.ALL);
            request.setAllowNoForecasts(false);
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> client().execute(DeleteForecastAction.INSTANCE, request).actionGet()
            );
            assertThat(
                e.getMessage(),
                equalTo("No forecast(s) [_all] exists for job [forecasts-delete-with-all-and-not-allow-no-forecasts]")
            );
        }
    }

    public void testForceStopSetsForecastToFailed() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-failed-on-force-stop");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);
        String jobId = job.getId();

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            data.add(createJsonRecord(createRecord(timestamp, 10.0)));
            data.add(createJsonRecord(createRecord(timestamp, 30.0)));
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);

        String forecastId = forecast(jobId, TimeValue.timeValueDays(1000), TimeValue.ZERO);
        waitForecastStatus(jobId, forecastId, ForecastRequestStats.ForecastRequestStatus.values());

        closeJob(jobId, true);
        // On force close job, it should always be at least failed or finished
        waitForecastStatus(
            jobId,
            forecastId,
            ForecastRequestStats.ForecastRequestStatus.FAILED,
            ForecastRequestStats.ForecastRequestStatus.FINISHED
        );
        ForecastRequestStats forecastStats = getForecastStats(job.getId(), forecastId);
        assertNotNull(forecastStats);
        if (forecastStats.getStatus().equals(ForecastRequestStats.ForecastRequestStatus.FAILED)) {
            assertThat(forecastStats.getMessages().get(0), equalTo(JOB_FORECAST_NATIVE_PROCESS_KILLED));
        }
    }

    public void testForecastWithHigherMemoryUse() throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("forecast-it-test-single-series");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 50 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            data.add(createJsonRecord(createRecord(timestamp, 10.0)));
            data.add(createJsonRecord(createRecord(timestamp, 30.0)));
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);

        // Now we can start doing forecast requests

        String forecastId = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO, ByteSizeValue.ofMb(50).getBytes());

        waitForecastToFinish(job.getId(), forecastId);
        closeJob(job.getId());

        List<ForecastRequestStats> forecastStats = getForecastStats();

        ForecastRequestStats forecastDuration1HourNoExpiry = forecastStats.get(0);
        assertThat(forecastDuration1HourNoExpiry.getExpiryTime(), equalTo(Instant.EPOCH));
        List<Forecast> forecasts = getForecasts(job.getId(), forecastDuration1HourNoExpiry);
        assertThat(forecastDuration1HourNoExpiry.getRecordCount(), equalTo(1L));
        assertThat(forecasts.size(), equalTo(1));
    }

    private void createDataWithLotsOfClientIps(TimeValue bucketSpan, Job.Builder job) {
        long now = Instant.now().getEpochSecond();
        long timestamp = now - 15 * bucketSpan.seconds();

        List<String> data = new ArrayList<>();
        for (int h = 0; h < 15; h++) {
            double value = 10.0 + h;
            for (int i = 1; i < 101; i++) {
                for (int j = 1; j < 81; j++) {
                    String json = Strings.format("""
                        {"time": %s, "value": %f, "clientIP": "192.168.%d.%d"}
                        """, timestamp, value, i, j);
                    data.add(json);
                }
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), false);
    }

    private static Map<String, Object> createRecord(long timestamp, double value) {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        record.put("value", value);
        return record;
    }

    private static Map<String, Object> createRecord(long timestamp, String partitionFieldValue, double value) {
        Map<String, Object> record = createRecord(timestamp, value);
        record.put("category", partitionFieldValue);
        return record;
    }
}
