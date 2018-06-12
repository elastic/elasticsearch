/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Condition;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleConditionType;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.junit.After;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

/**
 * An integration test for detection rules
 */
public class DetectionRulesIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() throws Exception {
        cleanUp();
    }

    @AwaitsFix(bugUrl = "this test is muted temporarily until the new rules implementation is merged in")
    public void testNumericalRule() throws Exception {
        RuleCondition condition1 = RuleCondition.createNumerical(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_1",
                new Condition(Operator.LT, "1000"));
        RuleCondition condition2 = RuleCondition.createNumerical(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_2",
                new Condition(Operator.LT, "500"));
        RuleCondition condition3 = RuleCondition.createNumerical(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_3",
                new Condition(Operator.LT, "100"));
        DetectionRule rule = new DetectionRule.Builder(Arrays.asList(condition1, condition2, condition3)).build();

        Detector.Builder detector = new Detector.Builder("max", "value");
        detector.setRules(Arrays.asList(rule));
        detector.setByFieldName("by_field");

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rule-numeric-test");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        registerJob(job);
        putJob(job);
        openJob(job.getId());

        long timestamp = 1491004800000L;
        int totalBuckets = 2 * 24;
        // each half of the buckets contains one anomaly for each by field value
        Set<Integer> anomalousBuckets = new HashSet<>(Arrays.asList(20, 44));
        List<String> byFieldValues = Arrays.asList("by_field_value_1", "by_field_value_2", "by_field_value_3");
        Map<String, Integer> anomalousValues = new HashMap<>();
        anomalousValues.put("by_field_value_1", 800);
        anomalousValues.put("by_field_value_2", 400);
        anomalousValues.put("by_field_value_3", 400);
        int normalValue = 1;
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
        closeJob(job.getId());

        List<AnomalyRecord> records = getRecords(job.getId());
        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getByFieldValue(), equalTo("by_field_value_3"));
        long firstRecordTimestamp = records.get(0).getTimestamp().getTime();

        {
            // Update rules so that the anomalies suppression is inverted
            RuleCondition newCondition1 = RuleCondition.createNumerical(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_1",
                    new Condition(Operator.GT, "1000"));
            RuleCondition newCondition2 = RuleCondition.createNumerical(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_2",
                    new Condition(Operator.GT, "500"));
            RuleCondition newCondition3 = RuleCondition.createNumerical(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_3",
                    new Condition(Operator.GT, "0"));
            DetectionRule newRule = new DetectionRule.Builder(Arrays.asList(newCondition1, newCondition2, newCondition3)).build();
            JobUpdate.Builder update = new JobUpdate.Builder(job.getId());
            update.setDetectorUpdates(Arrays.asList(new JobUpdate.DetectorUpdate(0, null, Arrays.asList(newRule))));
            updateJob(job.getId(), update.build());
        }

        // push second half
        openJob(job.getId());
        postData(job.getId(), joinBetween(data.size() / 2, data.size(), data));
        closeJob(job.getId());

        GetRecordsAction.Request recordsAfterFirstHalf = new GetRecordsAction.Request(job.getId());
        recordsAfterFirstHalf.setStart(String.valueOf(firstRecordTimestamp + 1));
        records = getRecords(recordsAfterFirstHalf);
        assertThat(records.size(), equalTo(2));
        Set<String> secondHaldRecordByFieldValues = records.stream().map(AnomalyRecord::getByFieldValue).collect(Collectors.toSet());
        assertThat(secondHaldRecordByFieldValues, contains("by_field_value_1", "by_field_value_2"));
    }

    @AwaitsFix(bugUrl = "this test is muted temporarily until the new rules implementation is merged in")
    public void testCategoricalRule() throws Exception {
        MlFilter safeIps = new MlFilter("safe_ips", Arrays.asList("111.111.111.111", "222.222.222.222"));
        assertThat(putMlFilter(safeIps), is(true));

        RuleCondition condition = RuleCondition.createCategorical("ip", safeIps.getId());
        DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(condition)).build();

        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setRules(Arrays.asList(rule));
        detector.setOverFieldName("ip");

        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        Job.Builder job = new Job.Builder("detection-rule-categorical-test");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        registerJob(job);
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
        MlFilter updatedFilter = new MlFilter(safeIps.getId(), Collections.singletonList("333.333.333.333"));
        assertThat(putMlFilter(updatedFilter), is(true));

        // Wait until the notification that the process was updated is indexed
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(".ml-notifications")
                    .setSize(1)
                    .addSort("timestamp", SortOrder.DESC)
                    .setQuery(QueryBuilders.boolQuery()
                                    .filter(QueryBuilders.termQuery("job_id", job.getId()))
                            .filter(QueryBuilders.termQuery("level", "info"))
                    ).get();
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(1));
            assertThat(hits[0].getSourceAsMap().get("message"), equalTo("Updated filter [safe_ips] in running process"));
        });

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
            assertThat(record.getOverFieldValue(), isOneOf("111.111.111.111", "222.222.222.222"));
        }

        closeJob(job.getId());
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
