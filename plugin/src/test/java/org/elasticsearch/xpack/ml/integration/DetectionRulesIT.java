/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.job.config.RuleConditionType;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * An integration test for detection rules
 */
public class DetectionRulesIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() throws Exception {
        cleanUp();
    }

    public void test() throws Exception {
        RuleCondition condition1 = new RuleCondition(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_1",
                new Condition(Operator.LT, "1000"),
                null);
        RuleCondition condition2 = new RuleCondition(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_2",
                new Condition(Operator.LT, "500"),
                null);
        RuleCondition condition3 = new RuleCondition(
                RuleConditionType.NUMERICAL_ACTUAL,
                "by_field",
                "by_field_value_3",
                new Condition(Operator.LT, "100"),
                null);
        DetectionRule rule = new DetectionRule.Builder(Arrays.asList(condition1, condition2, condition3)).build();

        Detector.Builder detector = new Detector.Builder("max", "value");
        detector.setDetectorRules(Arrays.asList(rule));
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
            RuleCondition newCondition1 = new RuleCondition(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_1",
                    new Condition(Operator.GT, "1000"),
                    null);
            RuleCondition newCondition2 = new RuleCondition(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_2",
                    new Condition(Operator.GT, "500"),
                    null);
            RuleCondition newCondition3 = new RuleCondition(
                    RuleConditionType.NUMERICAL_ACTUAL,
                    "by_field",
                    "by_field_value_3",
                    new Condition(Operator.GT, "0"),
                    null);
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

    private String joinBetween(int start, int end, List<String> input) {
        StringBuilder result = new StringBuilder();
        for (int i = start; i < end; i++) {
            result.append(input.get(i));
        }
        return result.toString();
    }
}
