/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleParams;
import org.elasticsearch.xpack.core.ml.job.config.RuleParamsForForceTimeShift;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

/**
 * An integration test for detection rules
 */
public class DetectionRulesIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testCondition() throws Exception {
        DetectionRule rule = new DetectionRule.Builder(Arrays.asList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.LT, 100.0)))
            .build();

        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setByFieldName("by_field");
        detector.setRules(Arrays.asList(rule));
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rules-it-test-condition");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long timestamp = 1491004800000L;
        int totalBuckets = 2 * 24;
        // each half of the buckets contains one anomaly for each by field value
        Set<Integer> anomalousBuckets = new HashSet<>(Arrays.asList(20, 44));
        List<String> byFieldValues = Arrays.asList("low", "high");
        Map<String, Integer> anomalousValues = new HashMap<>();
        anomalousValues.put("low", 99);
        anomalousValues.put("high", 701);
        int normalValue = 400;
        List<String> data = new ArrayList<>();
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            for (String byFieldValue : byFieldValues) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", timestamp);
                record.put("value", anomalousBuckets.contains(bucket) ? anomalousValues.get(byFieldValue) : normalValue);
                record.put("by_field", byFieldValue);
                data.add(createJsonRecord(record));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        // push the data for the first half buckets
        postData(job.getId(), joinBetween(0, data.size() / 2, data));
        flushJob(job.getId(), true);

        List<AnomalyRecord> records = getRecords(job.getId());
        // remove records that are not anomalies
        records.removeIf(record -> record.getInitialRecordScore() < 1e-5);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getByFieldValue(), equalTo("high"));
        long firstRecordTimestamp = records.get(0).getTimestamp().getTime();

        {
            // Update rules so that the anomalies suppression is inverted
            DetectionRule newRule = new DetectionRule.Builder(
                Arrays.asList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 700.0))
            ).build();
            JobUpdate.Builder update = new JobUpdate.Builder(job.getId());
            update.setDetectorUpdates(Arrays.asList(new JobUpdate.DetectorUpdate(0, null, Arrays.asList(newRule))));
            updateJob(job.getId(), update.build());
            // Wait until the notification that the job was updated is indexed
            assertBusy(
                () -> assertResponse(
                    prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).setSize(1)
                        .addSort("timestamp", SortOrder.DESC)
                        .setQuery(
                            QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("job_id", job.getId()))
                                .filter(QueryBuilders.termQuery("level", "info"))
                        ),
                    searchResponse -> {
                        SearchHit[] hits = searchResponse.getHits().getHits();
                        assertThat(hits.length, equalTo(1));
                        assertThat((String) hits[0].getSourceAsMap().get("message"), containsString("Job updated: [detectors]"));
                    }
                )
            );
        }

        // push second half
        postData(job.getId(), joinBetween(data.size() / 2, data.size(), data));
        flushJob(job.getId(), true);

        GetRecordsAction.Request recordsAfterFirstHalf = new GetRecordsAction.Request(job.getId());
        recordsAfterFirstHalf.setStart(String.valueOf(firstRecordTimestamp + 1));
        records = getRecords(recordsAfterFirstHalf);
        assertThat("records were " + records, (int) (records.stream().filter(r -> r.getProbability() < 0.01).count()), equalTo(1));
        assertThat(records.get(0).getByFieldValue(), equalTo("low"));
        closeJob(job.getId());
    }

    public void testScope() throws Exception {
        MlFilter safeIps = MlFilter.builder("safe_ips").setItems("111.111.111.111", "222.222.222.222").build();
        assertThat(putMlFilter(safeIps).getFilter(), equalTo(safeIps));

        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().include("ip", "safe_ips")).build();

        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setRules(Arrays.asList(rule));
        detector.setOverFieldName("ip");

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rules-it-test-scope");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long timestamp = 1509062400000L;
        List<String> data = new ArrayList<>();

        // Let's send a bunch of random IPs with counts of 1
        for (int bucket = 0; bucket < 20; bucket++) {
            for (int i = 0; i < 5; i++) {
                data.add(createIpRecord(timestamp, randomAlphaOfLength(10)));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        // Now send anomalous counts for our filtered IPs plus 333.333.333.333
        List<String> namedIps = Arrays.asList("111.111.111.111", "222.222.222.222", "333.333.333.333");
        long firstAnomalyTime = timestamp;
        for (int i = 0; i < 10; i++) {
            for (String ip : namedIps) {
                data.add(createIpRecord(timestamp, ip));
            }
        }

        // Some more normal buckets
        for (int bucket = 0; bucket < 3; bucket++) {
            for (int i = 0; i < 5; i++) {
                data.add(createIpRecord(timestamp, randomAlphaOfLength(10)));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        postData(job.getId(), joinBetween(0, data.size(), data));
        data = new ArrayList<>();
        flushJob(job.getId(), false);

        List<AnomalyRecord> records = getRecords(job.getId());
        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getTimestamp().getTime(), equalTo(firstAnomalyTime));
        assertThat(records.get(0).getOverFieldValue(), equalTo("333.333.333.333"));

        // Now let's update the filter
        UpdateFilterAction.Request updateFilterRequest = new UpdateFilterAction.Request(safeIps.getId());
        updateFilterRequest.setRemoveItems(safeIps.getItems());
        updateFilterRequest.setAddItems(Collections.singletonList("333.333.333.333"));
        client().execute(UpdateFilterAction.INSTANCE, updateFilterRequest).get();

        // Wait until the notification that the filter was updated is indexed
        assertBusy(
            () -> assertResponse(
                prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).setSize(1)
                    .addSort("timestamp", SortOrder.DESC)
                    .setQuery(
                        QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("job_id", job.getId()))
                            .filter(QueryBuilders.termQuery("level", "info"))
                    ),
                searchResponse -> {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    assertThat(hits.length, equalTo(1));
                    assertThat((String) hits[0].getSourceAsMap().get("message"), containsString("Filter [safe_ips] has been modified"));
                }
            )
        );

        long secondAnomalyTime = timestamp;
        // Send another anomalous bucket
        for (int i = 0; i < 10; i++) {
            for (String ip : namedIps) {
                data.add(createIpRecord(timestamp, ip));
            }
        }

        // Some more normal buckets
        for (int bucket = 0; bucket < 3; bucket++) {
            for (int i = 0; i < 5; i++) {
                data.add(createIpRecord(timestamp, randomAlphaOfLength(10)));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        postData(job.getId(), joinBetween(0, data.size(), data));
        flushJob(job.getId(), false);

        GetRecordsAction.Request getRecordsRequest = new GetRecordsAction.Request(job.getId());
        getRecordsRequest.setStart(Long.toString(firstAnomalyTime + 1));
        records = getRecords(getRecordsRequest);
        assertThat(records.size(), equalTo(2));
        for (AnomalyRecord record : records) {
            assertThat(record.getTimestamp().getTime(), equalTo(secondAnomalyTime));
            assertThat(record.getOverFieldValue(), is(oneOf("111.111.111.111", "222.222.222.222")));
        }

        closeJob(job.getId());
    }

    public void testScopeAndCondition() throws Exception {
        // We have 2 IPs and they're both safe-listed.
        List<String> ips = Arrays.asList("111.111.111.111", "222.222.222.222");
        MlFilter safeIps = MlFilter.builder("safe_ips").setItems(ips).build();
        assertThat(putMlFilter(safeIps).getFilter(), equalTo(safeIps));

        // Ignore if ip in safe list AND actual < 10.
        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().include("ip", "safe_ips")).setConditions(
            Arrays.asList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.LT, 10.0))
        ).build();

        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setRules(Arrays.asList(rule));
        detector.setOverFieldName("ip");

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rules-it-test-scope-and-condition");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long timestamp = 1509062400000L;
        List<String> data = new ArrayList<>();

        // First, 20 buckets with a count of 1 for both IPs
        for (int bucket = 0; bucket < 20; bucket++) {
            for (String ip : ips) {
                data.add(createIpRecord(timestamp, ip));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        // Now send anomalous count of 9 for 111.111.111.111
        for (int i = 0; i < 9; i++) {
            data.add(createIpRecord(timestamp, "111.111.111.111"));
        }

        // and 10 for 222.222.222.222
        for (int i = 0; i < 10; i++) {
            data.add(createIpRecord(timestamp, "222.222.222.222"));
        }
        timestamp += TimeValue.timeValueHours(1).getMillis();

        // Some more normal buckets
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String ip : ips) {
                data.add(createIpRecord(timestamp, ip));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        postData(job.getId(), joinBetween(0, data.size(), data));
        flushJob(job.getId(), true);

        List<AnomalyRecord> records = getRecords(job.getId());
        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getOverFieldValue(), equalTo("222.222.222.222"));

        // Remove "111.111.111.111" from the "safe_ips" filter
        List<String> addedIps = Arrays.asList();
        List<String> removedIps = Arrays.asList("111.111.111.111");
        PutFilterAction.Response updatedFilter = updateMlFilter("safe_ips", addedIps, removedIps);
        // Wait until the notification that the filter was updated is indexed
        assertBusy(
            () -> assertResponse(
                prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).setSize(1)
                    .addSort("timestamp", SortOrder.DESC)
                    .setQuery(
                        QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("job_id", job.getId()))
                            .filter(QueryBuilders.termQuery("level", "info"))
                    ),
                searchResponse -> {
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    assertThat(hits.length, equalTo(1));
                    assertThat(
                        (String) hits[0].getSourceAsMap().get("message"),
                        containsString("Filter [safe_ips] has been modified; removed items: ['111.111.111.111']")
                    );
                }
            )
        );
        MlFilter updatedSafeIps = MlFilter.builder("safe_ips").setItems(Arrays.asList("222.222.222.222")).build();
        assertThat(updatedFilter.getFilter(), equalTo(updatedSafeIps));

        data.clear();
        // Now send anomalous count of 9 for 111.111.111.111
        for (int i = 0; i < 9; i++) {
            data.add(createIpRecord(timestamp, "111.111.111.111"));
        }

        // Some more normal buckets
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String ip : ips) {
                data.add(createIpRecord(timestamp, ip));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        postData(job.getId(), joinBetween(0, data.size(), data));
        flushJob(job.getId(), true);

        records = getRecords(job.getId());
        assertThat(records.size(), equalTo(2));
        assertThat(records.get(0).getOverFieldValue(), equalTo("222.222.222.222"));
        assertThat(records.get(1).getOverFieldValue(), equalTo("111.111.111.111"));

        {
            // Update detection rules such that it now applies only to actual values > 10.0
            DetectionRule newRule = new DetectionRule.Builder(
                Arrays.asList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 10.0))
            ).build();
            JobUpdate.Builder update = new JobUpdate.Builder(job.getId());
            update.setDetectorUpdates(Arrays.asList(new JobUpdate.DetectorUpdate(0, null, Arrays.asList(newRule))));
            updateJob(job.getId(), update.build());
            // Wait until the notification that the job was updated is indexed
            assertBusy(
                () -> assertResponse(
                    prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).setSize(1)
                        .addSort("timestamp", SortOrder.DESC)
                        .setQuery(
                            QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("job_id", job.getId()))
                                .filter(QueryBuilders.termQuery("level", "info"))
                        ),
                    searchResponse -> {
                        SearchHit[] hits = searchResponse.getHits().getHits();
                        assertThat(hits.length, equalTo(1));
                        assertThat((String) hits[0].getSourceAsMap().get("message"), containsString("Job updated: [detectors]"));
                    }
                )
            );
        }

        data.clear();
        // Now send anomalous count of 10 for 222.222.222.222
        for (int i = 0; i < 10; i++) {
            data.add(createIpRecord(timestamp, "222.222.222.222"));
        }

        // Some more normal buckets
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String ip : ips) {
                data.add(createIpRecord(timestamp, ip));
            }
            timestamp += TimeValue.timeValueHours(1).getMillis();
        }

        postData(job.getId(), joinBetween(0, data.size(), data));

        closeJob(job.getId());

        // The anomalous records should not have changed.
        records = getRecords(job.getId());
        assertThat(records.size(), equalTo(2));
        assertThat(records.get(0).getOverFieldValue(), equalTo("222.222.222.222"));
        assertThat(records.get(1).getOverFieldValue(), equalTo("111.111.111.111"));

    }

    public void testForceTimeShiftAction() throws Exception {
        // The test ensures that the force time shift action works as expected.

        long timeShiftAmount = 3600L;
        long timestampStartMillis = 1491004800000L;
        long bucketSpanMillis = 3600000L;
        long timeShiftTimestamp = (timestampStartMillis + bucketSpanMillis) / 1000;

        int totalBuckets = 2 * 24;

        DetectionRule rule = new DetectionRule.Builder(
            Arrays.asList(new RuleCondition(RuleCondition.AppliesTo.TIME, Operator.GTE, timeShiftTimestamp))
        ).setActions(RuleAction.FORCE_TIME_SHIFT).setParams(new RuleParams(new RuleParamsForForceTimeShift(timeShiftAmount))).build();

        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setRules(Arrays.asList(rule));
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueMillis(bucketSpanMillis));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rules-it-test-force-time-shift");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        // post some data
        int normalValue = 400;
        List<String> data = new ArrayList<>();
        long timestamp = timestampStartMillis;
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            Map<String, Object> record = new HashMap<>();
            record.put("time", timestamp);
            record.put("value", normalValue);
            data.add(createJsonRecord(record));
            timestamp += bucketSpanMillis;
        }

        postData(job.getId(), joinBetween(0, data.size(), data));
        closeJob(job.getId());

        List<Annotation> annotations = getAnnotations();
        assertThat(annotations.size(), greaterThanOrEqualTo(1));
        assertThat(annotations.size(), lessThanOrEqualTo(3));

        // Check that annotation contain the expected time shift
        boolean countingModelAnnotationFound = false;
        boolean individualModelAnnotationFound = false;
        for (Annotation annotation : annotations) {
            if (annotation.getAnnotation().contains("Counting model shifted time by")) {
                countingModelAnnotationFound = true;
                assertThat(annotation.getAnnotation(), containsString(timeShiftAmount + " seconds"));
            } else if (annotation.getAnnotation().contains("Model shifted time by")) {
                individualModelAnnotationFound = true;
                assertThat(annotation.getAnnotation(), containsString(timeShiftAmount + " seconds"));
            }
        }
        assertThat("Counting model annotation with time shift not found", countingModelAnnotationFound, equalTo(true));
        assertThat("Individual model annotation with time shift not found", individualModelAnnotationFound, equalTo(true));
    }

    private String createIpRecord(long timestamp, String ip) throws IOException {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        record.put("ip", ip);
        return createJsonRecord(record);
    }

    private String joinBetween(int start, int end, List<String> input) {
        StringBuilder result = new StringBuilder();
        for (int i = start; i < end; i++) {
            result.append(input.get(i));
        }
        return result.toString();
    }
}
