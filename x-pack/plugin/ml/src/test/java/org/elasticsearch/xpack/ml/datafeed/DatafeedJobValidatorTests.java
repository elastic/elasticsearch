/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DatafeedJobValidatorTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testValidate_GivenNonZeroLatency() {
        String errorMessage = Messages.getMessage(Messages.DATAFEED_DOES_NOT_SUPPORT_JOB_WITH_LATENCY);
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        ac.setLatency(TimeValue.timeValueSeconds(3600));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfig().build();

        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry())
        );

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenZeroLatency() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        ac.setLatency(TimeValue.ZERO);
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfig().build();

        DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry());
    }

    public void testVerify_GivenNoLatency() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(TimeValue.timeValueSeconds(100));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfig().build();

        DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry());
    }

    public void testVerify_GivenAggsAndNoSummaryCountField() throws IOException {
        String errorMessage = Messages.getMessage(
            Messages.DATAFEED_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD,
            DatafeedConfig.DOC_COUNT
        );
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName(null);
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfigWithAggs(1800.0).build();

        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry())
        );

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenAggsAndEmptySummaryCountField() throws IOException {
        String errorMessage = Messages.getMessage(
            Messages.DATAFEED_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD,
            DatafeedConfig.DOC_COUNT
        );
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("");
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfigWithAggs(1800.0).build();

        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry())
        );

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenAggsAndSummaryCountField() throws IOException {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("some_count");
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfigWithAggs(900.0).build();
        DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry());
    }

    public void testVerify_GivenHistogramIntervalGreaterThanBucketSpan() throws IOException {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("some_count");
        ac.setBucketSpan(TimeValue.timeValueSeconds(1800));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfigWithAggs(1800001.0).build();

        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry())
        );

        assertEquals("Aggregation interval [1800001ms] must be less than or equal to the bucket_span [1800000ms]", e.getMessage());
    }

    public void testVerify_HistogramIntervalIsDivisorOfBucketSpan() throws IOException {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("some_count");
        ac.setBucketSpan(TimeValue.timeValueMinutes(5));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig datafeedConfig = createValidDatafeedConfigWithAggs(37 * 1000).build();

        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedConfig, job, xContentRegistry())
        );
        assertEquals("Aggregation interval [37000ms] must be a divisor of the bucket_span [300000ms]", e.getMessage());

        DatafeedConfig goodDatafeedConfig = createValidDatafeedConfigWithAggs(60 * 1000).build();
        DatafeedJobValidator.validate(goodDatafeedConfig, job, xContentRegistry());
    }

    public void testVerify_FrequencyIsMultipleOfHistogramInterval() throws IOException {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("some_count");
        ac.setBucketSpan(TimeValue.timeValueMinutes(5));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig.Builder datafeedBuilder = createValidDatafeedConfigWithAggs(60 * 1000);

        // Check with multiples
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(60));
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(120));
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(180));
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(240));
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());
        datafeedBuilder.setFrequency(TimeValue.timeValueHours(1));
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());

        // Now non-multiples
        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(30));
        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry())
        );
        assertEquals("Datafeed frequency [30s] must be a multiple of the aggregation interval [60000ms]", e.getMessage());

        datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(90));
        e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry())
        );
        assertEquals("Datafeed frequency [1.5m] must be a multiple of the aggregation interval [60000ms]", e.getMessage());
    }

    public void testVerify_BucketIntervalAndDataCheckWindowAreValid() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setSummaryCountFieldName("some_count");
        ac.setBucketSpan(TimeValue.timeValueSeconds(2));
        builder.setAnalysisConfig(ac);
        Job job = builder.build(new Date());
        DatafeedConfig.Builder datafeedBuilder = createValidDatafeedConfig();
        datafeedBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueMinutes(10)));

        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());

        datafeedBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueSeconds(1)));
        ElasticsearchStatusException e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry())
        );
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL, "1s", "2s"), e.getMessage());

        datafeedBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(24)));
        e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry())
        );
        assertEquals(Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_SPANS_TOO_MANY_BUCKETS, "1d", "2s"), e.getMessage());
    }

    public void testVerify_WithRuntimeTimeField() {
        String timeField = DataDescription.DEFAULT_TIME_FIELD;
        Job.Builder jobBuilder = buildJobBuilder("rt-time-field");
        DatafeedConfig.Builder datafeedBuilder = createValidDatafeedConfig();

        Map<String, Object> dateProperties = new HashMap<>();
        dateProperties.put("type", "date");
        dateProperties.put("script", "");
        Map<String, Object> longProperties = new HashMap<>();
        longProperties.put("type", "long");
        longProperties.put("script", "");

        Map<String, Object> runtimeMappings = new HashMap<>();
        runtimeMappings.put(timeField, dateProperties);
        runtimeMappings.put("field-foo", longProperties);

        datafeedBuilder.setRuntimeMappings(runtimeMappings);

        Job job = jobBuilder.build(new Date());
        Exception e = ESTestCase.expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedJobValidator.validate(datafeedBuilder.build(), jobBuilder.build(), xContentRegistry())
        );
        assertEquals("data_description.time_field [" + timeField + "] cannot be a runtime field", e.getMessage());

        runtimeMappings.remove(timeField);
        DatafeedJobValidator.validate(datafeedBuilder.build(), job, xContentRegistry());
    }

    private static Job.Builder buildJobBuilder(String id) {
        Job.Builder builder = new Job.Builder(id);
        AnalysisConfig.Builder ac = createAnalysisConfig();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        return builder;
    }

    public static AnalysisConfig.Builder createAnalysisConfig() {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        return ac;
    }

    private static DatafeedConfig.Builder createValidDatafeedConfigWithAggs(double interval) throws IOException {
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        HistogramAggregationBuilder histogram = AggregationBuilders.histogram("time")
            .interval(interval)
            .field("time")
            .subAggregation(maxTime);
        DatafeedConfig.Builder datafeedConfig = createValidDatafeedConfig();
        datafeedConfig.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(histogram));
        return datafeedConfig;
    }

    private static DatafeedConfig.Builder createValidDatafeedConfig() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("my-datafeed", "my-job");
        builder.setIndices(Collections.singletonList("myIndex"));
        return builder;
    }
}
