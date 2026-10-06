/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.ModelTests;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetectionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenizationTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests.createRandomBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigSizeUsageTests extends ESTestCase {

    public void testCollectJobConfigSizesIncludesDescriptionHistogram() {
        Job.Builder builder = JobTests.buildJobBuilder("job-1");
        builder.setDescription("Anomaly detection for CPU usage");
        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(builder.build()));

        assertThat(configSizes.containsKey("description"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> description = (Map<String, Object>) configSizes.get("description");
        assertThat(description.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) description.get(StatsAccumulator.Fields.MAX), greaterThan(0.0));
    }

    public void testCollectJobConfigSizesWithNullDataDescription() {
        AnalysisConfig analysisConfig = JobTests.createAnalysisConfig().build();
        Job job = mock(Job.class);
        when(job.getDescription()).thenReturn("desc");
        when(job.getCustomSettings()).thenReturn(null);
        when(job.getGroups()).thenReturn(List.of());
        when(job.getInitialResultsIndexName()).thenReturn(null);
        when(job.getAnalysisConfig()).thenReturn(analysisConfig);
        when(job.getDataDescription()).thenReturn(null);

        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(job));
        assertThat(configSizes.containsKey("description"), is(true));
        assertThat(configSizes.containsKey("time_field"), is(true));
    }

    public void testCollectJobConfigSizesIncludesCategorizationFilterSizes() {
        AnalysisConfig analysisConfig = mock(AnalysisConfig.class);
        when(analysisConfig.getCategorizationFilters()).thenReturn(List.of("foo.*", "bar.*"));
        when(analysisConfig.getInfluencers()).thenReturn(List.of());
        when(analysisConfig.getCategorizationFieldName()).thenReturn(null);
        when(analysisConfig.getSummaryCountFieldName()).thenReturn(null);
        when(analysisConfig.getCategorizationAnalyzerConfig()).thenReturn(null);
        when(analysisConfig.getDetectors()).thenReturn(List.of());

        Job job = mock(Job.class);
        when(job.getDescription()).thenReturn(null);
        when(job.getCustomSettings()).thenReturn(null);
        when(job.getGroups()).thenReturn(List.of());
        when(job.getInitialResultsIndexName()).thenReturn(null);
        when(job.getAnalysisConfig()).thenReturn(analysisConfig);
        when(job.getDataDescription()).thenReturn(null);

        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(job));

        @SuppressWarnings("unchecked")
        Map<String, Object> categorizationFilters = (Map<String, Object>) configSizes.get("categorization_filters");
        assertThat(categorizationFilters.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) categorizationFilters.get(StatsAccumulator.Fields.TOTAL), equalTo(10.0));
    }

    public void testCollectJobConfigSizesIncludesDetectorRuleSizes() {
        AnalysisConfig.Builder analysisConfig = JobTests.createAnalysisConfig();
        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().include("client", "filter-1")).build();
        Detector.Builder detectorBuilder = new Detector.Builder(analysisConfig.build().getDetectors().get(0));
        detectorBuilder.setRules(List.of(rule));
        analysisConfig.setDetectors(List.of(detectorBuilder.build()));

        Job job = JobTests.buildJobBuilder("job-1").setAnalysisConfig(analysisConfig).build();
        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(job));

        @SuppressWarnings("unchecked")
        Map<String, Object> detectorRules = (Map<String, Object>) configSizes.get("detector_rules");
        assertThat(detectorRules.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat(
            (Double) detectorRules.get(StatsAccumulator.Fields.TOTAL),
            equalTo((double) MlConfigSizeUtils.toXContentApproxSizeBytes(rule))
        );
    }

    public void testCollectDatafeedConfigSizesIncludesScriptFieldsAndIndices() {
        Script script = new Script(ScriptType.INLINE, "painless", "doc['latency'].value", Map.of());
        SearchSourceBuilder.ScriptField scriptField = new SearchSourceBuilder.ScriptField("latency", script, false);
        DatafeedConfig datafeed = new DatafeedConfig.Builder("df-1", "job-1").setIndices(List.of("logs-001", "logs-002"))
            .setScriptFields(List.of(scriptField))
            .build();

        Map<String, Object> configSizes = MlConfigSizeUsage.collectDatafeedConfigSizes(List.of(datafeed));

        long expectedScriptFieldBytes = MlConfigSizeUtils.toXContentFragmentApproxSizeBytes(scriptField);
        @SuppressWarnings("unchecked")
        Map<String, Object> scriptFields = (Map<String, Object>) configSizes.get("script_fields");
        if (expectedScriptFieldBytes < 0L) {
            assertThat(scriptFields.get(SizeHistogramAccumulator.FAILURES), equalTo(1L));
        } else {
            assertThat(scriptFields.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
            assertThat((Double) scriptFields.get(StatsAccumulator.Fields.TOTAL), equalTo((double) expectedScriptFieldBytes));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> indices = (Map<String, Object>) configSizes.get("indices");
        assertThat(indices.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) indices.get(StatsAccumulator.Fields.TOTAL), equalTo(16.0));
    }

    public void testCollectFilterConfigSizesIncludesItems() {
        MlFilter filter = MlFilter.builder("filter-1").setItems(new TreeSet<>(List.of("alpha", "beta", "gamma"))).build();
        Map<String, Object> configSizes = MlConfigSizeUsage.collectFilterConfigSizes(List.of(filter));

        @SuppressWarnings("unchecked")
        Map<String, Object> itemsCount = (Map<String, Object>) configSizes.get("items_count");
        assertThat(itemsCount.get(StatsAccumulator.Fields.MAX), equalTo(3.0));

        @SuppressWarnings("unchecked")
        Map<String, Object> items = (Map<String, Object>) configSizes.get("items");
        assertThat(items.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) items.get(StatsAccumulator.Fields.TOTAL), equalTo(14.0));
    }

    public void testCollectTrainedModelVocabularySingleSamplePerModel() {
        FillMaskConfig inferenceConfig = new FillMaskConfig(
            VocabularyConfigTests.createRandom(),
            BertTokenizationTests.createRandom(),
            null,
            "results"
        );
        long expectedBytes = MlConfigSizeUtils.toXContentApproxSizeBytes(inferenceConfig.getVocabularyConfig()) + MlConfigSizeUtils
            .toXContentApproxSizeBytes(inferenceConfig.getTokenization());

        TrainedModelConfig model = TrainedModelConfigTests.createTestInstance("model-1", false).setInferenceConfig(inferenceConfig).build();

        Map<String, Object> configSizes = MlConfigSizeUsage.collectTrainedModelConfigSizes(List.of(model));

        @SuppressWarnings("unchecked")
        Map<String, Object> vocabulary = (Map<String, Object>) configSizes.get("vocabulary");
        assertThat(vocabulary.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) vocabulary.get(StatsAccumulator.Fields.TOTAL), equalTo((double) expectedBytes));
    }

    public void testCollectDataFrameAnalyticsConfigSizesIncludesAnalyzedFieldStringSizes() {
        DataFrameAnalyticsConfig config = createRandomBuilder("dfa-1", false, OutlierDetectionTests.createRandom()).setAnalyzedFields(
            FetchSourceContext.of(true, new String[] { "field.a", "field.b" }, new String[] { "exclude.me" })
        ).build();

        Map<String, Object> configSizes = MlConfigSizeUsage.collectDataFrameAnalyticsConfigSizes(List.of(config));

        @SuppressWarnings("unchecked")
        Map<String, Object> includes = (Map<String, Object>) configSizes.get("analyzed_fields_includes");
        assertThat(includes.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) includes.get(StatsAccumulator.Fields.TOTAL), equalTo(14.0));

        @SuppressWarnings("unchecked")
        Map<String, Object> excludes = (Map<String, Object>) configSizes.get("analyzed_fields_excludes");
        assertThat(excludes.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) excludes.get(StatsAccumulator.Fields.TOTAL), equalTo(10.0));
    }

    public void testPutConfigSizesSkipsEmptyMap() {
        Map<String, Object> usageEntry = new java.util.HashMap<>();
        MlConfigSizeUsage.putConfigSizes(usageEntry, Map.of());
        assertThat(usageEntry.isEmpty(), is(true));
    }

    public void testCollectCalendarConfigSizesIncludesDescription() {
        Calendar calendar = new Calendar("cal-1", List.of("job-1"), "holiday calendar");
        Map<String, Object> configSizes = MlConfigSizeUsage.collectCalendarConfigSizes(List.of(calendar));

        assertThat(configSizes.containsKey("description"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> description = (Map<String, Object>) configSizes.get("description");
        assertThat(description.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) description.get(StatsAccumulator.Fields.MAX), greaterThan(0.0));
    }

    public void testCollectScheduledEventConfigSizesIncludesDescriptionAndEventsPerCalendar() {
        ScheduledEvent event = new ScheduledEvent.Builder().description("planned outage")
            .calendarId("cal-1")
            .startTime(Instant.EPOCH)
            .endTime(Instant.EPOCH.plusSeconds(3600))
            .build();
        Map<String, Object> configSizes = MlConfigSizeUsage.collectScheduledEventConfigSizes(List.of(event));

        assertThat(configSizes.containsKey("description"), is(true));
        assertThat(configSizes.containsKey("events_per_calendar_count"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> eventsPerCalendar = (Map<String, Object>) configSizes.get("events_per_calendar_count");
        assertThat(eventsPerCalendar.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
    }

    public void testCollectModelSnapshotConfigSizesIncludesDescription() {
        ModelSnapshot snapshot = new ModelSnapshot.Builder().setJobId("job-1")
            .setDescription("baseline snapshot")
            .setSnapshotId("snap-1")
            .setTimestamp(new Date())
            .build();
        Map<String, Object> configSizes = MlConfigSizeUsage.collectModelSnapshotConfigSizes(List.of(snapshot));

        @SuppressWarnings("unchecked")
        Map<String, Object> description = (Map<String, Object>) configSizes.get("description");
        assertThat(description.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) description.get(StatsAccumulator.Fields.MAX), greaterThan(0.0));
    }

    public void testCollectInferenceEndpointConfigSizesUsesModelConfigurationFieldNames() {
        ModelConfigurations endpoint = ModelTests.randomModel().getConfigurations();
        Map<String, Object> configSizes = MlConfigSizeUsage.collectInferenceEndpointConfigSizes(List.of(endpoint));

        assertThat(configSizes.containsKey(ModelConfigurations.INFERENCE_ID_FIELD_NAME), is(true));
        assertThat(configSizes.containsKey(ModelConfigurations.SERVICE_SETTINGS), is(true));
        assertThat(configSizes.containsKey(ModelConfigurations.TASK_SETTINGS), is(true));
    }

    public void testCollectJobConfigSizesShouldCountSerializationFailures() throws IOException {
        CategorizationAnalyzerConfig analyzer = mock(CategorizationAnalyzerConfig.class);
        doAnswer(invocation -> { throw new IOException("serialization failed"); }).when(analyzer)
            .toXContent(any(XContentBuilder.class), any());

        AnalysisConfig analysisConfig = mock(AnalysisConfig.class);
        when(analysisConfig.getCategorizationFilters()).thenReturn(List.of());
        when(analysisConfig.getInfluencers()).thenReturn(List.of());
        when(analysisConfig.getCategorizationFieldName()).thenReturn(null);
        when(analysisConfig.getSummaryCountFieldName()).thenReturn(null);
        when(analysisConfig.getCategorizationAnalyzerConfig()).thenReturn(analyzer);
        when(analysisConfig.getDetectors()).thenReturn(List.of());

        Job job = mock(Job.class);
        when(job.getDescription()).thenReturn(null);
        when(job.getCustomSettings()).thenReturn(null);
        when(job.getGroups()).thenReturn(List.of());
        when(job.getInitialResultsIndexName()).thenReturn(null);
        when(job.getAnalysisConfig()).thenReturn(analysisConfig);
        when(job.getDataDescription()).thenReturn(null);

        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(job));

        @SuppressWarnings("unchecked")
        Map<String, Object> categorizationAnalyzer = (Map<String, Object>) configSizes.get("categorization_analyzer");
        assertThat(categorizationAnalyzer.get(SizeHistogramAccumulator.FAILURES), equalTo(1L));
    }
}
