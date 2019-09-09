/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyCause;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influence;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.notifications.AnomalyDetectionAuditMessage;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Static methods to create Elasticsearch index mappings for the autodetect
 * persisted objects/documents and configurations
 * <p>
 * ElasticSearch automatically recognises array types so they are
 * not explicitly mapped as such. For arrays of objects the type
 * must be set to <i>nested</i> so the arrays are searched properly
 * see https://www.elastic.co/guide/en/elasticsearch/guide/current/nested-objects.html
 * <p>
 * It is expected that indexes to which these mappings are applied have their
 * default analyzer set to "keyword", which does not tokenise fields.  The
 * index-wide default analyzer cannot be set via these mappings, so needs to be
 * set in the index settings during index creation. For the results mapping the
 * _all field is disabled and a custom all field is used in its place. The index
 * settings must have {@code "index.query.default_field": "all_field_values" } set
 * for the queries to use the custom all field. The custom all field has its
 * analyzer set to "whitespace" by these mappings, so that it gets tokenised
 * using whitespace.
 */
public class ElasticsearchMappings {

    /**
     * String constants used in mappings
     */
    public static final String ENABLED = "enabled";
    public static final String ANALYZER = "analyzer";
    public static final String WHITESPACE = "whitespace";
    public static final String NESTED = "nested";
    public static final String COPY_TO = "copy_to";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";
    public static final String DYNAMIC = "dynamic";
    public static final String FIELDS = "fields";

    /**
     * Name of the custom 'all' field for results
     */
    public static final String ALL_FIELD_VALUES = "all_field_values";

    /**
     * Name of the Elasticsearch field by which documents are sorted by default
     */
    public static final String ES_DOC = "_doc";

    /**
     * The configuration document type
     */
    public static final String CONFIG_TYPE = "config_type";

    /**
     * Elasticsearch data types
     */
    public static final String BOOLEAN = "boolean";
    public static final String DATE = "date";
    public static final String DOUBLE = "double";
    public static final String INTEGER = "integer";
    public static final String KEYWORD = "keyword";
    public static final String LONG = "long";
    public static final String TEXT = "text";

    static final String RAW = "raw";

    private static final Logger logger = LogManager.getLogger(ElasticsearchMappings.class);

    private ElasticsearchMappings() {
    }

    public static XContentBuilder configMapping() throws IOException {
        return configMapping(SINGLE_MAPPING_NAME);
    }

    public static XContentBuilder configMapping(String mappingType) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject(mappingType);
        addMetaInformation(builder);
        addDefaultMapping(builder);
        builder.startObject(PROPERTIES);

        addJobConfigFields(builder);
        addDatafeedConfigFields(builder);
        addDataFrameAnalyticsFields(builder);

        builder.endObject()
               .endObject()
               .endObject();
        return builder;
    }

    public static void addJobConfigFields(XContentBuilder builder) throws IOException {

        builder.startObject(CONFIG_TYPE)
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.JOB_TYPE.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.JOB_VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.GROUPS.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.ANALYSIS_CONFIG.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(AnalysisConfig.BUCKET_SPAN.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.CATEGORIZATION_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.CATEGORIZATION_ANALYZER.getPreferredName())
                    .field(ENABLED, false)
                .endObject()
                .startObject(AnalysisConfig.LATENCY.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.SUMMARY_COUNT_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.DETECTORS.getPreferredName())
                    .startObject(PROPERTIES)
                        .startObject(Detector.DETECTOR_DESCRIPTION_FIELD.getPreferredName())
                            .field(TYPE, TEXT)
                        .endObject()
                        .startObject(Detector.FUNCTION_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.FIELD_NAME_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.BY_FIELD_NAME_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.OVER_FIELD_NAME_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.PARTITION_FIELD_NAME_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.USE_NULL_FIELD.getPreferredName())
                            .field(TYPE, BOOLEAN)
                        .endObject()
                        .startObject(Detector.EXCLUDE_FREQUENT_FIELD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Detector.CUSTOM_RULES_FIELD.getPreferredName())
                            .field(TYPE, NESTED)
                            .startObject(PROPERTIES)
                                .startObject(DetectionRule.ACTIONS_FIELD.getPreferredName())
                                    .field(TYPE, KEYWORD)
                                .endObject()
                                // RuleScope is a map
                                .startObject(DetectionRule.SCOPE_FIELD.getPreferredName())
                                    .field(ENABLED, false)
                                .endObject()
                                .startObject(DetectionRule.CONDITIONS_FIELD.getPreferredName())
                                    .field(TYPE, NESTED)
                                    .startObject(PROPERTIES)
                                        .startObject(RuleCondition.APPLIES_TO_FIELD.getPreferredName())
                                            .field(TYPE, KEYWORD)
                                        .endObject()
                                        .startObject(Operator.OPERATOR_FIELD.getPreferredName())
                                            .field(TYPE, KEYWORD)
                                        .endObject()
                                        .startObject(RuleCondition.VALUE_FIELD.getPreferredName())
                                            .field(TYPE, DOUBLE)
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject(Detector.DETECTOR_INDEX.getPreferredName())
                            .field(TYPE, INTEGER)
                        .endObject()
                    .endObject()
                .endObject()

                .startObject(AnalysisConfig.INFLUENCERS.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnalysisConfig.MULTIVARIATE_BY_FIELDS.getPreferredName())
                    .field(TYPE, BOOLEAN)
                .endObject()
            .endObject()
        .endObject()

        .startObject(Job.ANALYSIS_LIMITS.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(AnalysisLimits.MODEL_MEMORY_LIMIT.getPreferredName())
                    .field(TYPE, KEYWORD)  // TODO Should be a ByteSizeValue
                .endObject()
                .startObject(AnalysisLimits.CATEGORIZATION_EXAMPLES_LIMIT.getPreferredName())
                    .field(TYPE, LONG)
                .endObject()
            .endObject()
        .endObject()

        .startObject(Job.CREATE_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()

        .startObject(Job.CUSTOM_SETTINGS.getPreferredName())
            // Custom settings are an untyped map
            .field(ENABLED, false)
        .endObject()

        .startObject(Job.DATA_DESCRIPTION.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(DataDescription.FORMAT_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataDescription.TIME_FIELD_NAME_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataDescription.TIME_FORMAT_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataDescription.FIELD_DELIMITER_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataDescription.QUOTE_CHARACTER_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
            .endObject()
        .endObject()

        .startObject(Job.DESCRIPTION.getPreferredName())
            .field(TYPE, TEXT)
        .endObject()
        .startObject(Job.FINISHED_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()

        .startObject(Job.MODEL_PLOT_CONFIG.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(ModelPlotConfig.ENABLED_FIELD.getPreferredName())
                    .field(TYPE, BOOLEAN)
                .endObject()
                .startObject(ModelPlotConfig.TERMS_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
            .endObject()
        .endObject()

        .startObject(Job.RENORMALIZATION_WINDOW_DAYS.getPreferredName())
            .field(TYPE, LONG) // TODO should be TimeValue
        .endObject()
        .startObject(Job.BACKGROUND_PERSIST_INTERVAL.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.MODEL_SNAPSHOT_RETENTION_DAYS.getPreferredName())
            .field(TYPE, LONG) // TODO should be TimeValue
        .endObject()
        .startObject(Job.RESULTS_RETENTION_DAYS.getPreferredName())
            .field(TYPE, LONG)  // TODO should be TimeValue
        .endObject()
        .startObject(Job.MODEL_SNAPSHOT_ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.MODEL_SNAPSHOT_MIN_VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Job.RESULTS_INDEX_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject();
    }

    public static void addDatafeedConfigFields(XContentBuilder builder) throws IOException {
        builder.startObject(DatafeedConfig.ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(DatafeedConfig.QUERY_DELAY.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(DatafeedConfig.FREQUENCY.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(DatafeedConfig.INDICES.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(DatafeedConfig.QUERY.getPreferredName())
            .field(ENABLED, false)
        .endObject()
        .startObject(DatafeedConfig.SCROLL_SIZE.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DatafeedConfig.AGGREGATIONS.getPreferredName())
            .field(ENABLED, false)
        .endObject()
        .startObject(DatafeedConfig.SCRIPT_FIELDS.getPreferredName())
            .field(ENABLED, false)
        .endObject()
        .startObject(DatafeedConfig.CHUNKING_CONFIG.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(ChunkingConfig.MODE_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ChunkingConfig.TIME_SPAN_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
            .endObject()
        .endObject()
        .startObject(DatafeedConfig.DELAYED_DATA_CHECK_CONFIG.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(DelayedDataCheckConfig.ENABLED.getPreferredName())
                    .field(TYPE, BOOLEAN)
                .endObject()
                .startObject(DelayedDataCheckConfig.CHECK_WINDOW.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
            .endObject()
        .endObject()
        .startObject(DatafeedConfig.HEADERS.getPreferredName())
            .field(ENABLED, false)
        .endObject();
    }

    /**
     * {@link DataFrameAnalyticsConfig} mapping.
     * Does not include mapping for CREATE_TIME as this mapping is added by {@link #addJobConfigFields} method.
     */
    public static void addDataFrameAnalyticsFields(XContentBuilder builder) throws IOException {
        builder.startObject(DataFrameAnalyticsConfig.ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(DataFrameAnalyticsConfig.SOURCE.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(DataFrameAnalyticsSource.INDEX.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataFrameAnalyticsSource.QUERY.getPreferredName())
                    .field(ENABLED, false)
                .endObject()
            .endObject()
        .endObject()
        .startObject(DataFrameAnalyticsConfig.DEST.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(DataFrameAnalyticsDest.INDEX.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
            .endObject()
        .endObject()
        .startObject(DataFrameAnalyticsConfig.ANALYZED_FIELDS.getPreferredName())
            .field(ENABLED, false)
        .endObject()
        .startObject(DataFrameAnalyticsConfig.ANALYSIS.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(OutlierDetection.NAME.getPreferredName())
                    .startObject(PROPERTIES)
                        .startObject(OutlierDetection.N_NEIGHBORS.getPreferredName())
                            .field(TYPE, INTEGER)
                        .endObject()
                        .startObject(OutlierDetection.METHOD.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(OutlierDetection.FEATURE_INFLUENCE_THRESHOLD.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                    .endObject()
                .endObject()
                .startObject(Regression.NAME.getPreferredName())
                    .startObject(PROPERTIES)
                        .startObject(Regression.DEPENDENT_VARIABLE.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Regression.LAMBDA.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                        .startObject(Regression.GAMMA.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                        .startObject(Regression.ETA.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                        .startObject(Regression.MAXIMUM_NUMBER_TREES.getPreferredName())
                            .field(TYPE, INTEGER)
                        .endObject()
                        .startObject(Regression.FEATURE_BAG_FRACTION.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                        .startObject(Regression.PREDICTION_FIELD_NAME.getPreferredName())
                            .field(TYPE, KEYWORD)
                        .endObject()
                        .startObject(Regression.TRAINING_PERCENT.getPreferredName())
                            .field(TYPE, DOUBLE)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
        .endObject()
        // re-used: CREATE_TIME
        .startObject(DataFrameAnalyticsConfig.VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject();
    }

    /**
     * Creates a default mapping which has a dynamic template that
     * treats all dynamically added fields as keywords. This is needed
     * so that the per-job term fields will not be automatically added
     * as fields of type 'text' to the index mappings of newly rolled indices.
     *
     * @throws IOException On write error
     */
    public static void addDefaultMapping(XContentBuilder builder) throws IOException {
        builder.startArray("dynamic_templates")
                    .startObject()
                        .startObject("strings_as_keywords")
                            .field("match", "*")
                            .startObject("mapping")
                                .field(TYPE, KEYWORD)
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray();
    }

    /**
     * Inserts "_meta" containing useful information like the version into the mapping
     * template.
     *
     * @param builder The builder for the mappings
     * @throws IOException On write error
     */
    public static void addMetaInformation(XContentBuilder builder) throws IOException {
        builder.startObject("_meta")
                    .field("version", Version.CURRENT)
               .endObject();
    }

    public static XContentBuilder resultsMapping(String mappingType) throws IOException {
        return resultsMapping(mappingType, Collections.emptyList());
    }

    public static XContentBuilder resultsMapping(String mappingType, Collection<String> extraTermFields) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject(mappingType);
        addMetaInformation(builder);
        addDefaultMapping(builder);
        builder.startObject(PROPERTIES);

        // Add result all field for easy searches in kibana
        builder.startObject(ALL_FIELD_VALUES)
            .field(TYPE, TEXT)
            .field(ANALYZER, WHITESPACE)
        .endObject();

        builder.startObject(Job.ID.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject();

        builder.startObject(Result.TIMESTAMP.getPreferredName())
            .field(TYPE, DATE)
        .endObject();

        addResultsMapping(builder);
        addCategoryDefinitionMapping(builder);
        addDataCountsMapping(builder);
        addTimingStatsExceptBucketCountMapping(builder);
        addDatafeedTimingStats(builder);
        addModelSnapshotMapping(builder);

        addTermFields(builder, extraTermFields);

        // end properties
        builder.endObject();
        // end type
        builder.endObject();
        // end mapping
        builder.endObject();


        return builder;
    }

    /**
     * Create the Elasticsearch mapping for results objects
     *  {@link Bucket}s, {@link AnomalyRecord}s, {@link Influencer} and
     * {@link BucketInfluencer}
     *
     * The mapping has a custom all field containing the *_FIELD_VALUE fields
     * e.g. BY_FIELD_VALUE, OVER_FIELD_VALUE, etc. The custom all field {@link #ALL_FIELD_VALUES}
     * must be set in the index settings. A custom all field is preferred over the usual
     * '_all' field as most fields do not belong in '_all', disabling '_all' and
     * using a custom all field simplifies the mapping.
     *
     * These fields are copied to the custom all field
     * <ul>
     *     <li>by_field_value</li>
     *     <li>partition_field_value</li>
     *     <li>over_field_value</li>
     *     <li>AnomalyCause.correlated_by_field_value</li>
     *     <li>AnomalyCause.by_field_value</li>
     *     <li>AnomalyCause.partition_field_value</li>
     *     <li>AnomalyCause.over_field_value</li>
     *     <li>AnomalyRecord.Influencers.influencer_field_values</li>
     *     <li>Influencer.influencer_field_value</li>
     * </ul>
     *
     * @throws IOException On write error
     */
    private static void addResultsMapping(XContentBuilder builder) throws IOException {
        builder.startObject(Result.RESULT_TYPE.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject()
            .startObject(Bucket.ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(BucketInfluencer.RAW_ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(Bucket.INITIAL_ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(Result.IS_INTERIM.getPreferredName())
                .field(TYPE, BOOLEAN)
            .endObject()
            .startObject(Bucket.EVENT_COUNT.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
            .startObject(Bucket.BUCKET_SPAN.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
            .startObject(Bucket.PROCESSING_TIME_MS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
            .startObject(Bucket.SCHEDULED_EVENTS.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject()

            .startObject(Bucket.BUCKET_INFLUENCERS.getPreferredName())
                .field(TYPE, NESTED)
                .startObject(PROPERTIES)
                    .startObject(Job.ID.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                    .startObject(Result.RESULT_TYPE.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                    .startObject(BucketInfluencer.INFLUENCER_FIELD_NAME.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                    .startObject(BucketInfluencer.INITIAL_ANOMALY_SCORE.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                    .startObject(BucketInfluencer.ANOMALY_SCORE.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                    .startObject(BucketInfluencer.RAW_ANOMALY_SCORE.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                    .startObject(BucketInfluencer.PROBABILITY.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                    .startObject(Result.TIMESTAMP.getPreferredName())
                        .field(TYPE, DATE)
                    .endObject()
                    .startObject(BucketInfluencer.BUCKET_SPAN.getPreferredName())
                        .field(TYPE, LONG)
                    .endObject()
                    .startObject(Result.IS_INTERIM.getPreferredName())
                        .field(TYPE, BOOLEAN)
                    .endObject()
                .endObject()
            .endObject()

            // Model Plot Output
            .startObject(ModelPlot.MODEL_FEATURE.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject()
            .startObject(ModelPlot.MODEL_LOWER.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(ModelPlot.MODEL_UPPER.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(ModelPlot.MODEL_MEDIAN.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject();

        addForecastFieldsToMapping(builder);
        addAnomalyRecordFieldsToMapping(builder);
        addInfluencerFieldsToMapping(builder);
        addModelSizeStatsFieldsToMapping(builder);
    }

    /**
     * Generate a keyword mapping for {@code termFields} for the default type
     * {@link org.elasticsearch.index.mapper.MapperService#SINGLE_MAPPING_NAME}
     *
     * If the returned mapping is used in index creation and the new index has a matching template
     * then the mapping type ({@link org.elasticsearch.index.mapper.MapperService#SINGLE_MAPPING_NAME})
     * must match the mapping type of the template otherwise the mappings will not be merged correctly.
     *
     * @param termFields Fields to generate mapping for
     * @return The mapping
     */
    public static XContentBuilder termFieldsMapping(Collection<String> termFields) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
            builder.startObject(PROPERTIES);
            addTermFields(builder, termFields);
            builder.endObject();
            builder.endObject();
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addTermFields(XContentBuilder builder, Collection<String> termFields) throws IOException {
        for (String fieldName : termFields) {
            if (ReservedFieldNames.isValidFieldName(fieldName)) {
                builder.startObject(fieldName).field(TYPE, KEYWORD).endObject();
            }
        }
    }

    private static void addForecastFieldsToMapping(XContentBuilder builder) throws IOException {

        // Forecast Output
        builder.startObject(Forecast.FORECAST_LOWER.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(Forecast.FORECAST_UPPER.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(Forecast.FORECAST_PREDICTION.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(Forecast.FORECAST_ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject();

        // Forecast Stats Output
        // re-used: TIMESTAMP, PROCESSING_TIME_MS, PROCESSED_RECORD_COUNT, LATEST_RECORD_TIME
        builder.startObject(ForecastRequestStats.START_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(ForecastRequestStats.END_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(ForecastRequestStats.CREATE_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(ForecastRequestStats.EXPIRY_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(ForecastRequestStats.MESSAGES.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(ForecastRequestStats.PROGRESS.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(ForecastRequestStats.STATUS.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(ForecastRequestStats.MEMORY_USAGE.getPreferredName())
            .field(TYPE, LONG)
        .endObject();
    }

    /**
     * AnomalyRecord fields to be added under the 'properties' section of the mapping
     * @param builder Add properties to this builder
     * @throws IOException On write error
     */
    private static void addAnomalyRecordFieldsToMapping(XContentBuilder builder) throws IOException {
        builder.startObject(Detector.DETECTOR_INDEX.getPreferredName())
            .field(TYPE, INTEGER)
        .endObject()
        .startObject(AnomalyRecord.ACTUAL.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.TYPICAL.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.PROBABILITY.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.MULTI_BUCKET_IMPACT.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.FUNCTION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.FUNCTION_DESCRIPTION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.BY_FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.BY_FIELD_VALUE.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject()
        .startObject(AnomalyRecord.FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.PARTITION_FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject()
        .startObject(AnomalyRecord.OVER_FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(AnomalyRecord.OVER_FIELD_VALUE.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject()
        .startObject(AnomalyRecord.RECORD_SCORE.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.INITIAL_RECORD_SCORE.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.CAUSES.getPreferredName())
            .field(TYPE, NESTED)
            .startObject(PROPERTIES)
                .startObject(AnomalyCause.ACTUAL.getPreferredName())
                    .field(TYPE, DOUBLE)
                .endObject()
                .startObject(AnomalyCause.TYPICAL.getPreferredName())
                    .field(TYPE, DOUBLE)
                .endObject()
                .startObject(AnomalyCause.PROBABILITY.getPreferredName())
                    .field(TYPE, DOUBLE)
                .endObject()
                .startObject(AnomalyCause.FUNCTION.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.FUNCTION_DESCRIPTION.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.BY_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.BY_FIELD_VALUE.getPreferredName())
                    .field(TYPE, KEYWORD)
                    .field(COPY_TO, ALL_FIELD_VALUES)
                .endObject()
                .startObject(AnomalyCause.CORRELATED_BY_FIELD_VALUE.getPreferredName())
                    .field(TYPE, KEYWORD)
                    .field(COPY_TO, ALL_FIELD_VALUES)
                .endObject()
                .startObject(AnomalyCause.FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.PARTITION_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.PARTITION_FIELD_VALUE.getPreferredName())
                    .field(TYPE, KEYWORD)
                    .field(COPY_TO, ALL_FIELD_VALUES)
                .endObject()
                .startObject(AnomalyCause.OVER_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.OVER_FIELD_VALUE.getPreferredName())
                    .field(TYPE, KEYWORD)
                    .field(COPY_TO, ALL_FIELD_VALUES)
                .endObject()
            .endObject()
        .endObject()
        .startObject(AnomalyRecord.INFLUENCERS.getPreferredName())
            /* Array of influences */
            .field(TYPE, NESTED)
            .startObject(PROPERTIES)
                .startObject(Influence.INFLUENCER_FIELD_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(Influence.INFLUENCER_FIELD_VALUES.getPreferredName())
                    .field(TYPE, KEYWORD)
                    .field(COPY_TO, ALL_FIELD_VALUES)
                .endObject()
            .endObject()
        .endObject();
    }

    private static void addInfluencerFieldsToMapping(XContentBuilder builder) throws IOException {
        builder.startObject(Influencer.INFLUENCER_SCORE.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(Influencer.INITIAL_INFLUENCER_SCORE.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(Influencer.INFLUENCER_FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Influencer.INFLUENCER_FIELD_VALUE.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject();
    }

    /**
     * {@link DataCounts} mapping.
     *
     * @throws IOException On builder write error
     */
    private static void addDataCountsMapping(XContentBuilder builder) throws IOException {
        builder.startObject(DataCounts.PROCESSED_RECORD_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.PROCESSED_FIELD_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.INPUT_BYTES.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.INPUT_RECORD_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.INPUT_FIELD_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.INVALID_DATE_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.MISSING_FIELD_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.OUT_OF_ORDER_TIME_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.EMPTY_BUCKET_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.SPARSE_BUCKET_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.BUCKET_COUNT.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(DataCounts.EARLIEST_RECORD_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(DataCounts.LATEST_RECORD_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(DataCounts.LATEST_EMPTY_BUCKET_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(DataCounts.LATEST_SPARSE_BUCKET_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(DataCounts.LAST_DATA_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject();
    }

    /**
     * {@link TimingStats} mapping.
     * Does not include mapping for BUCKET_COUNT as this mapping is added by {@link #addDataCountsMapping} method.
     *
     * @throws IOException On builder write error
     */
    private static void addTimingStatsExceptBucketCountMapping(XContentBuilder builder) throws IOException {
        builder
            // re-used: BUCKET_COUNT
            .startObject(TimingStats.MIN_BUCKET_PROCESSING_TIME_MS.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TimingStats.MAX_BUCKET_PROCESSING_TIME_MS.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TimingStats.AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TimingStats.EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName())
                .field(TYPE, DOUBLE)
            .endObject()
            .startObject(TimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName())
                .startObject(PROPERTIES)
                    .startObject(ExponentialAverageCalculationContext.INCREMENTAL_METRIC_VALUE_MS.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                    .startObject(ExponentialAverageCalculationContext.LATEST_TIMESTAMP.getPreferredName())
                        .field(TYPE, DATE)
                    .endObject()
                    .startObject(ExponentialAverageCalculationContext.PREVIOUS_EXPONENTIAL_AVERAGE_MS.getPreferredName())
                        .field(TYPE, DOUBLE)
                    .endObject()
                .endObject()
            .endObject();
    }

    /**
     * {@link DatafeedTimingStats} mapping.
     * Does not include mapping for BUCKET_COUNT as this mapping is added by {@link #addDataCountsMapping} method.
     * Does not include mapping for EXPONENTIAL_AVG_CALCULATION_CONTEXT as this mapping is added by
     *     {@link #addTimingStatsExceptBucketCountMapping} method.
     *
     * @throws IOException On builder write error
     */
    private static void addDatafeedTimingStats(XContentBuilder builder) throws IOException {
        builder
            .startObject(DatafeedTimingStats.SEARCH_COUNT.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
            // re-used: BUCKET_COUNT
            .startObject(DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName())
                .field(TYPE, DOUBLE)
            // re-used: EXPONENTIAL_AVG_CALCULATION_CONTEXT
            .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain CategoryDefinition}.
     * The '_all' field is disabled as the document isn't meant to be searched.
     *
     * @throws IOException On builder error
     */
    private static void addCategoryDefinitionMapping(XContentBuilder builder) throws IOException {
        builder.startObject(CategoryDefinition.CATEGORY_ID.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(CategoryDefinition.TERMS.getPreferredName())
            .field(TYPE, TEXT)
        .endObject()
        .startObject(CategoryDefinition.REGEX.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(CategoryDefinition.EXAMPLES.getPreferredName())
            .field(TYPE, TEXT)
        .endObject();
    }

    /**
     * Create the Elasticsearch mapping for state.  State could potentially be
     * huge (target document size is 16MB and there can be many documents) so all
     * analysis by Elasticsearch is disabled.  The only way to retrieve state is
     * by knowing the ID of a particular document.
     */
    public static XContentBuilder stateMapping() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject(SINGLE_MAPPING_NAME);
        addMetaInformation(builder);
        builder.field(ENABLED, false);
        builder.endObject();
        builder.endObject();

        return builder;
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain ModelSnapshot}.
     * The '_all' field is disabled but the type is searchable
     */
    private static void addModelSnapshotMapping(XContentBuilder builder) throws IOException {
        builder.startObject(ModelSnapshot.DESCRIPTION.getPreferredName())
            .field(TYPE, TEXT)
        .endObject()
        .startObject(ModelSnapshotField.SNAPSHOT_ID.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName())
            .field(TYPE, INTEGER)
        .endObject()
        .startObject(ModelSnapshot.RETAIN.getPreferredName())
            .field(TYPE, BOOLEAN)
        .endObject()
        .startObject(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName())
            .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(Result.RESULT_TYPE.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ModelSizeStats.TIMESTAMP_FIELD.getPreferredName())
                    .field(TYPE, DATE)
                .endObject();

        addModelSizeStatsFieldsToMapping(builder);

        // end model size stats properties
        builder.endObject();
        // end model size stats mapping
        builder.endObject();

        builder.startObject(ModelSnapshot.QUANTILES.getPreferredName())
            .field(ENABLED, false)
        .endObject().startObject(ModelSnapshot.MIN_VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(ModelSnapshot.LATEST_RECORD_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject()
        .startObject(ModelSnapshot.LATEST_RESULT_TIME.getPreferredName())
            .field(TYPE, DATE)
        .endObject();
    }

    /**
     * {@link ModelSizeStats} fields to be added under the 'properties' section of the mapping
     * @param builder Add properties to this builder
     * @throws IOException On write error
     */
    private static void addModelSizeStatsFieldsToMapping(XContentBuilder builder) throws IOException {
        builder.startObject(ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(ModelSizeStats.TOTAL_BY_FIELD_COUNT_FIELD.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(ModelSizeStats.TOTAL_OVER_FIELD_COUNT_FIELD.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(ModelSizeStats.TOTAL_PARTITION_FIELD_COUNT_FIELD.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(ModelSizeStats.BUCKET_ALLOCATION_FAILURES_COUNT_FIELD.getPreferredName())
            .field(TYPE, LONG)
        .endObject()
        .startObject(ModelSizeStats.MEMORY_STATUS_FIELD.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(ModelSizeStats.LOG_TIME_FIELD.getPreferredName())
            .field(TYPE, DATE)
        .endObject();
    }

    public static XContentBuilder auditMessageMapping() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject(SINGLE_MAPPING_NAME);
        addMetaInformation(builder);
        builder.startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyDetectionAuditMessage.LEVEL.getPreferredName())
                   .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyDetectionAuditMessage.MESSAGE.getPreferredName())
                    .field(TYPE, TEXT)
                    .startObject(FIELDS)
                        .startObject(RAW)
                            .field(TYPE, KEYWORD)
                        .endObject()
                    .endObject()
                .endObject()
                .startObject(AnomalyDetectionAuditMessage.TIMESTAMP.getPreferredName())
                    .field(TYPE, DATE)
                .endObject()
                .startObject(AnomalyDetectionAuditMessage.NODE_NAME.getPreferredName())
                    .field(TYPE, KEYWORD)
                .endObject()
        .endObject()
        .endObject()
        .endObject();

        return builder;
    }

    static String[] mappingRequiresUpdate(ClusterState state, String[] concreteIndices, Version minVersion) throws IOException {
        List<String> indicesToUpdate = new ArrayList<>();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> currentMapping = state.metaData().findMappings(concreteIndices,
                new String[0], MapperPlugin.NOOP_FIELD_FILTER);

        for (String index : concreteIndices) {
            ImmutableOpenMap<String, MappingMetaData> innerMap = currentMapping.get(index);
            if (innerMap != null) {
                MappingMetaData metaData = innerMap.valuesIt().next();
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> meta = (Map<String, Object>) metaData.sourceAsMap().get("_meta");
                    if (meta != null) {
                        String versionString = (String) meta.get("version");
                        if (versionString == null) {
                            logger.info("Version of mappings for [{}] not found, recreating", index);
                            indicesToUpdate.add(index);
                            continue;
                        }

                        Version mappingVersion = Version.fromString(versionString);

                        if (mappingVersion.onOrAfter(minVersion)) {
                            continue;
                        } else {
                            logger.info("Mappings for [{}] are outdated [{}], updating it[{}].", index, mappingVersion, Version.CURRENT);
                            indicesToUpdate.add(index);
                            continue;
                        }
                    } else {
                        logger.info("Version of mappings for [{}] not found, recreating", index);
                        indicesToUpdate.add(index);
                        continue;
                    }
                } catch (Exception e) {
                    logger.error(new ParameterizedMessage("Failed to retrieve mapping version for [{}], recreating", index), e);
                    indicesToUpdate.add(index);
                    continue;
                }
            } else {
                logger.info("No mappings found for [{}], recreating", index);
                indicesToUpdate.add(index);
            }
        }
        return indicesToUpdate.toArray(new String[indicesToUpdate.size()]);
    }

    public static void addDocMappingIfMissing(String alias,
                                              CheckedFunction<String, XContentBuilder, IOException> mappingSupplier,
                                              Client client, ClusterState state, ActionListener<Boolean> listener) {
        AliasOrIndex aliasOrIndex = state.metaData().getAliasAndIndexLookup().get(alias);
        if (aliasOrIndex == null) {
            // The index has never been created yet
            listener.onResponse(true);
            return;
        }
        String[] concreteIndices = aliasOrIndex.getIndices().stream().map(IndexMetaData::getIndex).map(Index::getName)
            .toArray(String[]::new);

        String[] indicesThatRequireAnUpdate;
        try {
            indicesThatRequireAnUpdate = mappingRequiresUpdate(state, concreteIndices, Version.CURRENT);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        if (indicesThatRequireAnUpdate.length > 0) {
            // Use the mapping type of the first index in the update
            IndexMetaData indexMetaData = state.metaData().index(indicesThatRequireAnUpdate[0]);
            String mappingType = indexMetaData.mapping().type();

            try (XContentBuilder mapping = mappingSupplier.apply(mappingType)) {
                PutMappingRequest putMappingRequest = new PutMappingRequest(indicesThatRequireAnUpdate);
                putMappingRequest.type(mappingType);
                putMappingRequest.source(mapping);
                executeAsyncWithOrigin(client, ML_ORIGIN, PutMappingAction.INSTANCE, putMappingRequest,
                    ActionListener.wrap(response -> {
                        if (response.isAcknowledged()) {
                            listener.onResponse(true);
                        } else {
                            listener.onFailure(new ElasticsearchException("Attempt to put missing mapping in indices "
                                + Arrays.toString(indicesThatRequireAnUpdate) + " was not acknowledged"));
                        }
                    }, listener::onFailure));
            } catch (IOException e) {
                listener.onFailure(e);
            }
        } else {
            logger.trace("Mappings are up to date.");
            listener.onResponse(true);
        }
    }
}
