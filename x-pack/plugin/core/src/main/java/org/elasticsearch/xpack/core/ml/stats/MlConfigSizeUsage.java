/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects histogram telemetry for unbounded ML config field sizes.
 */
public final class MlConfigSizeUsage {

    public static final String CONFIG_SIZES = "config_sizes";

    private MlConfigSizeUsage() {}

    public static Map<String, Object> collectJobConfigSizes(List<Job> jobs) {
        Map<String, SizeHistogramAccumulator> accumulators = newJobConfigAccumulators();
        for (Job job : jobs) {
            accumulateJobConfigSizes(job, accumulators);
        }
        return MlConfigSizeUtils.configSizesMap(accumulators);
    }

    public static Map<String, Object> collectDatafeedConfigSizes(List<DatafeedConfig> datafeeds) {
        Map<String, SizeHistogramAccumulator> accumulators = newDatafeedConfigAccumulators();
        for (DatafeedConfig datafeed : datafeeds) {
            accumulateDatafeedConfigSizes(datafeed, accumulators);
        }
        return MlConfigSizeUtils.configSizesMap(accumulators);
    }

    public static Map<String, Object> collectDataFrameAnalyticsConfigSizes(List<DataFrameAnalyticsConfig> configs) {
        Map<String, SizeHistogramAccumulator> accumulators = newDataFrameAnalyticsConfigAccumulators();
        for (DataFrameAnalyticsConfig config : configs) {
            accumulateDataFrameAnalyticsConfigSizes(config, accumulators);
        }
        return MlConfigSizeUtils.configSizesMap(accumulators);
    }

    public static Map<String, Object> collectTrainedModelConfigSizes(List<TrainedModelConfig> trainedModelConfigs) {
        Map<String, SizeHistogramAccumulator> accumulators = newTrainedModelConfigAccumulators();
        for (TrainedModelConfig trainedModelConfig : trainedModelConfigs) {
            accumulateTrainedModelConfigSizes(trainedModelConfig, accumulators);
        }
        return MlConfigSizeUtils.configSizesMap(accumulators);
    }

    public static Map<String, Object> collectInferenceEndpointConfigSizes(List<ModelConfigurations> endpoints) {
        Map<String, SizeHistogramAccumulator> accumulators = newInferenceEndpointConfigAccumulators();
        for (ModelConfigurations endpoint : endpoints) {
            accumulateInferenceEndpointConfigSizes(endpoint, accumulators);
        }
        return MlConfigSizeUtils.configSizesMap(accumulators);
    }

    public static Map<String, Object> collectCalendarConfigSizes(List<Calendar> calendars) {
        SizeHistogramAccumulator description = new SizeHistogramAccumulator();
        SizeHistogramAccumulator jobIdsCount = new SizeHistogramAccumulator();
        for (Calendar calendar : calendars) {
            description.add(MlConfigSizeUtils.stringLength(calendar.getDescription()));
            jobIdsCount.add(MlConfigSizeUtils.collectionCount(calendar.getJobIds()));
        }
        return MlConfigSizeUtils.configSizesMap(Map.of("description", description, "job_ids_count", jobIdsCount));
    }

    public static Map<String, Object> collectScheduledEventConfigSizes(List<ScheduledEvent> events) {
        SizeHistogramAccumulator description = new SizeHistogramAccumulator();
        SizeHistogramAccumulator eventsPerCalendar = new SizeHistogramAccumulator();
        Map<String, Long> eventsByCalendar = new HashMap<>();
        for (ScheduledEvent event : events) {
            description.add(MlConfigSizeUtils.stringLength(event.getDescription()));
            eventsByCalendar.merge(event.getCalendarId(), 1L, Long::sum);
        }
        eventsByCalendar.values().forEach(eventsPerCalendar::add);
        return MlConfigSizeUtils.configSizesMap(Map.of("description", description, "events_per_calendar_count", eventsPerCalendar));
    }

    public static Map<String, Object> collectFilterConfigSizes(List<MlFilter> filters) {
        SizeHistogramAccumulator description = new SizeHistogramAccumulator();
        SizeHistogramAccumulator itemsCount = new SizeHistogramAccumulator();
        SizeHistogramAccumulator items = new SizeHistogramAccumulator();
        for (MlFilter filter : filters) {
            description.add(MlConfigSizeUtils.stringLength(filter.getDescription()));
            itemsCount.add(MlConfigSizeUtils.collectionCount(filter.getItems()));
            items.add(MlConfigSizeUtils.stringCollectionTotalLength(filter.getItems()));
        }
        return MlConfigSizeUtils.configSizesMap(Map.of("description", description, "items_count", itemsCount, "items", items));
    }

    public static Map<String, Object> collectModelSnapshotConfigSizes(List<ModelSnapshot> snapshots) {
        SizeHistogramAccumulator description = new SizeHistogramAccumulator();
        for (ModelSnapshot snapshot : snapshots) {
            description.add(MlConfigSizeUtils.stringLength(snapshot.getDescription()));
        }
        return MlConfigSizeUtils.configSizesMap(Map.of("description", description));
    }

    private static Map<String, SizeHistogramAccumulator> newJobConfigAccumulators() {
        Map<String, SizeHistogramAccumulator> accumulators = new LinkedHashMap<>();
        accumulators.put("description", new SizeHistogramAccumulator());
        accumulators.put("custom_settings", new SizeHistogramAccumulator());
        accumulators.put("groups_count", new SizeHistogramAccumulator());
        accumulators.put("results_index_name", new SizeHistogramAccumulator());
        accumulators.put("categorization_filters_count", new SizeHistogramAccumulator());
        accumulators.put("categorization_filters", new SizeHistogramAccumulator());
        accumulators.put("influencers_count", new SizeHistogramAccumulator());
        accumulators.put("categorization_field_name", new SizeHistogramAccumulator());
        accumulators.put("summary_count_field_name", new SizeHistogramAccumulator());
        accumulators.put("categorization_analyzer", new SizeHistogramAccumulator());
        accumulators.put("detector_description", new SizeHistogramAccumulator());
        accumulators.put("detector_rules_count", new SizeHistogramAccumulator());
        accumulators.put("detector_rules", new SizeHistogramAccumulator());
        accumulators.put("detector_field_name", new SizeHistogramAccumulator());
        accumulators.put("time_field", new SizeHistogramAccumulator());
        accumulators.put("time_format", new SizeHistogramAccumulator());
        return accumulators;
    }

    private static void accumulateJobConfigSizes(Job job, Map<String, SizeHistogramAccumulator> accumulators) {
        accumulators.get("description").add(MlConfigSizeUtils.stringLength(job.getDescription()));
        accumulators.get("custom_settings").add(MlConfigSizeUtils.mapApproxSizeBytes(job.getCustomSettings()));
        accumulators.get("groups_count").add(MlConfigSizeUtils.collectionCount(job.getGroups()));
        accumulators.get("results_index_name").add(MlConfigSizeUtils.stringLength(job.getInitialResultsIndexName()));

        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        accumulators.get("categorization_filters_count").add(MlConfigSizeUtils.collectionCount(analysisConfig.getCategorizationFilters()));
        accumulators.get("categorization_filters")
            .add(MlConfigSizeUtils.stringCollectionTotalLength(analysisConfig.getCategorizationFilters()));
        accumulators.get("influencers_count").add(MlConfigSizeUtils.collectionCount(analysisConfig.getInfluencers()));
        accumulators.get("categorization_field_name").add(MlConfigSizeUtils.stringLength(analysisConfig.getCategorizationFieldName()));
        accumulators.get("summary_count_field_name").add(MlConfigSizeUtils.stringLength(analysisConfig.getSummaryCountFieldName()));
        CategorizationAnalyzerConfig categorizationAnalyzer = analysisConfig.getCategorizationAnalyzerConfig();
        if (categorizationAnalyzer != null) {
            accumulators.get("categorization_analyzer").add(MlConfigSizeUtils.toXContentFragmentApproxSizeBytes(categorizationAnalyzer));
        }

        for (Detector detector : analysisConfig.getDetectors()) {
            accumulators.get("detector_description").add(MlConfigSizeUtils.stringLength(detector.getDetectorDescription()));
            accumulators.get("detector_rules_count").add(MlConfigSizeUtils.collectionCount(detector.getRules()));
            List<DetectionRule> rules = detector.getRules();
            if (rules.isEmpty() == false) {
                long detectorRulesBytes = 0L;
                for (DetectionRule rule : rules) {
                    detectorRulesBytes = MlConfigSizeUtils.sumSizeBytes(
                        detectorRulesBytes,
                        MlConfigSizeUtils.toXContentApproxSizeBytes(rule)
                    );
                }
                accumulators.get("detector_rules").add(detectorRulesBytes);
            }
            addFieldNameLength(accumulators.get("detector_field_name"), detector.getFieldName());
            addFieldNameLength(accumulators.get("detector_field_name"), detector.getByFieldName());
            addFieldNameLength(accumulators.get("detector_field_name"), detector.getOverFieldName());
            addFieldNameLength(accumulators.get("detector_field_name"), detector.getPartitionFieldName());
        }

        DataDescription dataDescription = job.getDataDescription();
        if (dataDescription != null) {
            accumulators.get("time_field").add(MlConfigSizeUtils.stringLength(dataDescription.getTimeField()));
            accumulators.get("time_format").add(MlConfigSizeUtils.stringLength(dataDescription.getTimeFormat()));
        }
    }

    private static Map<String, SizeHistogramAccumulator> newDatafeedConfigAccumulators() {
        Map<String, SizeHistogramAccumulator> accumulators = new LinkedHashMap<>();
        accumulators.put("query", new SizeHistogramAccumulator());
        accumulators.put("aggregations", new SizeHistogramAccumulator());
        accumulators.put("runtime_mappings", new SizeHistogramAccumulator());
        accumulators.put("script_fields_count", new SizeHistogramAccumulator());
        accumulators.put("script_fields", new SizeHistogramAccumulator());
        accumulators.put("indices_count", new SizeHistogramAccumulator());
        accumulators.put("indices", new SizeHistogramAccumulator());
        return accumulators;
    }

    private static void accumulateDatafeedConfigSizes(DatafeedConfig datafeed, Map<String, SizeHistogramAccumulator> accumulators) {
        accumulators.get("query").add(MlConfigSizeUtils.mapApproxSizeBytes(datafeed.getQuery()));
        accumulators.get("aggregations").add(MlConfigSizeUtils.mapApproxSizeBytes(datafeed.getAggregations()));
        accumulators.get("runtime_mappings").add(MlConfigSizeUtils.mapApproxSizeBytes(datafeed.getRuntimeMappings()));
        accumulators.get("script_fields_count").add(MlConfigSizeUtils.collectionCount(datafeed.getScriptFields()));
        List<SearchSourceBuilder.ScriptField> scriptFields = datafeed.getScriptFields();
        if (scriptFields.isEmpty() == false) {
            long scriptFieldBytes = 0L;
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptFieldBytes = MlConfigSizeUtils.sumSizeBytes(
                    scriptFieldBytes,
                    MlConfigSizeUtils.toXContentFragmentApproxSizeBytes(scriptField)
                );
            }
            accumulators.get("script_fields").add(scriptFieldBytes);
        }
        accumulators.get("indices_count").add(MlConfigSizeUtils.collectionCount(datafeed.getIndices()));
        accumulators.get("indices").add(MlConfigSizeUtils.stringCollectionTotalLength(datafeed.getIndices()));
    }

    private static Map<String, SizeHistogramAccumulator> newDataFrameAnalyticsConfigAccumulators() {
        Map<String, SizeHistogramAccumulator> accumulators = new LinkedHashMap<>();
        accumulators.put("description", new SizeHistogramAccumulator());
        accumulators.put("meta", new SizeHistogramAccumulator());
        accumulators.put("feature_processors_count", new SizeHistogramAccumulator());
        accumulators.put("feature_processors", new SizeHistogramAccumulator());
        accumulators.put("analyzed_fields_includes_count", new SizeHistogramAccumulator());
        accumulators.put("analyzed_fields_excludes_count", new SizeHistogramAccumulator());
        accumulators.put("analyzed_fields_includes", new SizeHistogramAccumulator());
        accumulators.put("analyzed_fields_excludes", new SizeHistogramAccumulator());
        accumulators.put("source_query", new SizeHistogramAccumulator());
        accumulators.put("source_runtime_mappings", new SizeHistogramAccumulator());
        accumulators.put("dest_results_field", new SizeHistogramAccumulator());
        return accumulators;
    }

    private static void accumulateDataFrameAnalyticsConfigSizes(
        DataFrameAnalyticsConfig config,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        accumulators.get("description").add(MlConfigSizeUtils.stringLength(config.getDescription()));
        accumulators.get("meta").add(MlConfigSizeUtils.mapApproxSizeBytes(config.getMeta()));

        DataFrameAnalysis analysis = config.getAnalysis();
        accumulateFeatureProcessorSizes(analysis, accumulators);
        accumulateAnalyzedFieldSizes(config.getAnalyzedFields(), accumulators);
        accumulateDataFrameAnalyticsSourceSizes(config.getSource(), accumulators);
        accumulateDataFrameAnalyticsDestSizes(config.getDest(), accumulators);
    }

    private static void accumulateFeatureProcessorSizes(DataFrameAnalysis analysis, Map<String, SizeHistogramAccumulator> accumulators) {
        List<PreProcessor> featureProcessors = featureProcessors(analysis);
        accumulators.get("feature_processors_count").add(MlConfigSizeUtils.collectionCount(featureProcessors));
        if (featureProcessors.isEmpty()) {
            return;
        }
        long featureProcessorBytes = 0L;
        for (PreProcessor featureProcessor : featureProcessors) {
            featureProcessorBytes = MlConfigSizeUtils.sumSizeBytes(
                featureProcessorBytes,
                MlConfigSizeUtils.toXContentApproxSizeBytes(featureProcessor)
            );
        }
        accumulators.get("feature_processors").add(featureProcessorBytes);
    }

    private static void accumulateAnalyzedFieldSizes(
        @Nullable FetchSourceContext analyzedFields,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        if (analyzedFields == null) {
            return;
        }
        String[] includes = analyzedFields.includes();
        String[] excludes = analyzedFields.excludes();
        accumulators.get("analyzed_fields_includes_count").add(includes == null ? 0L : includes.length);
        accumulators.get("analyzed_fields_excludes_count").add(excludes == null ? 0L : excludes.length);
        accumulators.get("analyzed_fields_includes").add(MlConfigSizeUtils.stringArrayTotalLength(includes));
        accumulators.get("analyzed_fields_excludes").add(MlConfigSizeUtils.stringArrayTotalLength(excludes));
    }

    private static void accumulateDataFrameAnalyticsSourceSizes(
        @Nullable DataFrameAnalyticsSource source,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        if (source == null) {
            return;
        }
        long sourceQueryBytes = MlConfigSizeUtils.queryBuilderApproxSizeBytes(source.getParsedQuery());
        accumulators.get("source_query").add(sourceQueryBytes);
        accumulators.get("source_runtime_mappings").add(MlConfigSizeUtils.mapApproxSizeBytes(source.getRuntimeMappings()));
    }

    private static void accumulateDataFrameAnalyticsDestSizes(
        @Nullable DataFrameAnalyticsDest dest,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        if (dest == null) {
            return;
        }
        accumulators.get("dest_results_field").add(MlConfigSizeUtils.stringLength(dest.getResultsField()));
    }

    private static List<PreProcessor> featureProcessors(DataFrameAnalysis analysis) {
        if (analysis instanceof Classification classification) {
            return classification.getFeatureProcessors();
        }
        if (analysis instanceof Regression regression) {
            return regression.getFeatureProcessors();
        }
        return List.of();
    }

    private static Map<String, SizeHistogramAccumulator> newTrainedModelConfigAccumulators() {
        Map<String, SizeHistogramAccumulator> accumulators = new LinkedHashMap<>();
        accumulators.put("description", new SizeHistogramAccumulator());
        accumulators.put("metadata", new SizeHistogramAccumulator());
        accumulators.put("tags_count", new SizeHistogramAccumulator());
        accumulators.put("tags", new SizeHistogramAccumulator());
        accumulators.put("input_field_names_count", new SizeHistogramAccumulator());
        accumulators.put("input_field_name", new SizeHistogramAccumulator());
        accumulators.put("classification_labels_count", new SizeHistogramAccumulator());
        accumulators.put("classification_labels", new SizeHistogramAccumulator());
        accumulators.put("default_field_map", new SizeHistogramAccumulator());
        accumulators.put("prefix_strings", new SizeHistogramAccumulator());
        accumulators.put("vocabulary", new SizeHistogramAccumulator());
        return accumulators;
    }

    private static void accumulateTrainedModelConfigSizes(
        TrainedModelConfig trainedModelConfig,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        accumulators.get("description").add(MlConfigSizeUtils.stringLength(trainedModelConfig.getDescription()));
        accumulators.get("metadata").add(MlConfigSizeUtils.mapApproxSizeBytes(trainedModelConfig.getMetadata()));
        accumulators.get("tags_count").add(MlConfigSizeUtils.collectionCount(trainedModelConfig.getTags()));
        accumulators.get("tags").add(MlConfigSizeUtils.stringCollectionTotalLength(trainedModelConfig.getTags()));
        accumulators.get("default_field_map").add(MlConfigSizeUtils.mapApproxSizeBytes(trainedModelConfig.getDefaultFieldMap()));
        accumulators.get("prefix_strings").add(MlConfigSizeUtils.toXContentApproxSizeBytes(trainedModelConfig.getPrefixStrings()));

        TrainedModelInput input = trainedModelConfig.getInput();
        if (input != null) {
            accumulators.get("input_field_names_count").add(MlConfigSizeUtils.collectionCount(input.getFieldNames()));
            for (String fieldName : input.getFieldNames()) {
                accumulators.get("input_field_name").add(MlConfigSizeUtils.stringLength(fieldName));
            }
        }

        InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig();
        if (inferenceConfig instanceof NlpConfig nlpConfig) {
            accumulators.get("classification_labels_count").add(classificationLabelsCount(inferenceConfig));
            accumulators.get("classification_labels")
                .add(MlConfigSizeUtils.stringCollectionTotalLength(classificationLabels(inferenceConfig)));
            VocabularyConfig vocabularyConfig = nlpConfig.getVocabularyConfig();
            Tokenization tokenization = nlpConfig.getTokenization();
            if (vocabularyConfig != null || tokenization != null) {
                long vocabularyBytes = 0L;
                if (vocabularyConfig != null) {
                    vocabularyBytes = MlConfigSizeUtils.sumSizeBytes(
                        vocabularyBytes,
                        MlConfigSizeUtils.toXContentApproxSizeBytes(vocabularyConfig)
                    );
                }
                if (tokenization != null) {
                    vocabularyBytes = MlConfigSizeUtils.sumSizeBytes(
                        vocabularyBytes,
                        MlConfigSizeUtils.toXContentApproxSizeBytes(tokenization)
                    );
                }
                accumulators.get("vocabulary").add(vocabularyBytes);
            }
        }
    }

    private static Map<String, SizeHistogramAccumulator> newInferenceEndpointConfigAccumulators() {
        Map<String, SizeHistogramAccumulator> accumulators = new LinkedHashMap<>();
        accumulators.put(ModelConfigurations.INFERENCE_ID_FIELD_NAME, new SizeHistogramAccumulator());
        accumulators.put(ModelConfigurations.SERVICE_SETTINGS, new SizeHistogramAccumulator());
        accumulators.put(ModelConfigurations.TASK_SETTINGS, new SizeHistogramAccumulator());
        accumulators.put(ModelConfigurations.CHUNKING_SETTINGS, new SizeHistogramAccumulator());
        return accumulators;
    }

    private static void accumulateInferenceEndpointConfigSizes(
        ModelConfigurations endpoint,
        Map<String, SizeHistogramAccumulator> accumulators
    ) {
        accumulators.get(ModelConfigurations.INFERENCE_ID_FIELD_NAME).add(MlConfigSizeUtils.stringLength(endpoint.getInferenceEntityId()));
        ServiceSettings serviceSettings = endpoint.getServiceSettings();
        if (serviceSettings != null) {
            accumulators.get(ModelConfigurations.SERVICE_SETTINGS).add(MlConfigSizeUtils.toXContentApproxSizeBytes(serviceSettings));
        }
        TaskSettings taskSettings = endpoint.getTaskSettings();
        if (taskSettings != null) {
            accumulators.get(ModelConfigurations.TASK_SETTINGS).add(MlConfigSizeUtils.toXContentApproxSizeBytes(taskSettings));
        }
        ChunkingSettings chunkingSettings = endpoint.getChunkingSettings();
        if (chunkingSettings != null) {
            accumulators.get(ModelConfigurations.CHUNKING_SETTINGS).add(MlConfigSizeUtils.toXContentApproxSizeBytes(chunkingSettings));
        }
    }

    private static long classificationLabelsCount(InferenceConfig inferenceConfig) {
        return MlConfigSizeUtils.collectionCount(classificationLabels(inferenceConfig));
    }

    @Nullable
    private static Collection<String> classificationLabels(InferenceConfig inferenceConfig) {
        if (inferenceConfig instanceof TextClassificationConfig textClassificationConfig) {
            return textClassificationConfig.getClassificationLabels();
        }
        if (inferenceConfig instanceof NerConfig nerConfig) {
            return nerConfig.getClassificationLabels();
        }
        if (inferenceConfig instanceof ZeroShotClassificationConfig zeroShotClassificationConfig) {
            return zeroShotClassificationConfig.getClassificationLabels();
        }
        return null;
    }

    private static void addFieldNameLength(SizeHistogramAccumulator accumulator, String fieldName) {
        if (fieldName != null) {
            accumulator.add(MlConfigSizeUtils.stringLength(fieldName));
        }
    }

    public static void putConfigSizes(Map<String, Object> usageEntry, Map<String, Object> configSizes) {
        if (configSizes.isEmpty() == false) {
            usageEntry.put(CONFIG_SIZES, configSizes);
        }
    }

    public static void putAuxiliaryConfigSizes(Map<String, Object> jobsUsage, String key, Map<String, Object> configSizes) {
        if (configSizes.isEmpty() == false) {
            jobsUsage.put(key, Map.of(CONFIG_SIZES, configSizes));
        }
    }
}
