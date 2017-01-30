/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.ml.notifications.AuditActivity;
import org.elasticsearch.xpack.ml.notifications.AuditMessage;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.AnomalyCause;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Influence;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.ModelDebugOutput;
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Static methods to create Elasticsearch mappings for the autodetect
 * persisted objects/documents
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
    static final String ENABLED = "enabled";
    static final String ANALYZER = "analyzer";
    static final String WHITESPACE = "whitespace";
    static final String NESTED = "nested";
    static final String COPY_TO = "copy_to";
    static final String PROPERTIES = "properties";
    static final String TYPE = "type";
    static final String DYNAMIC = "dynamic";

    /**
     * Name of the custom 'all' field for results
     */
    public static final String ALL_FIELD_VALUES = "all_field_values";

    /**
     * Name of the Elasticsearch field by which documents are sorted by default
     */
    static final String ES_DOC = "_doc";

    /**
     * Elasticsearch data types
     */
    static final String BOOLEAN = "boolean";
    static final String DATE = "date";
    static final String DOUBLE = "double";
    static final String INTEGER = "integer";
    static final String KEYWORD = "keyword";
    static final String LONG = "long";
    static final String OBJECT = "object";
    static final String TEXT = "text";

    private ElasticsearchMappings() {
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
     * @param termFieldNames All the term fields (by, over, partition) and influencers
     *                       included in the mapping
     *
     * @return The mapping
     * @throws IOException On write error
     */
    public static XContentBuilder resultsMapping(Collection<String> termFieldNames) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                    .startObject(Result.TYPE.getPreferredName())
                        .startObject(PROPERTIES)
                            .startObject(ALL_FIELD_VALUES)
                                .field(TYPE, TEXT)
                                .field(ANALYZER, WHITESPACE)
                            .endObject()
                            .startObject(Result.RESULT_TYPE.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(Job.ID.getPreferredName())
                                .field(TYPE, KEYWORD)
                                .field(COPY_TO, ALL_FIELD_VALUES)
                            .endObject()
                            .startObject(Bucket.TIMESTAMP.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                            .startObject(Bucket.ANOMALY_SCORE.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject()
                            .startObject(Bucket.INITIAL_ANOMALY_SCORE.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject()
                            .startObject(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject()
                            .startObject(Bucket.IS_INTERIM.getPreferredName())
                                .field(TYPE, BOOLEAN)
                            .endObject()
                            .startObject(Bucket.RECORD_COUNT.getPreferredName())
                                .field(TYPE, LONG)
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
                            .startObject(Bucket.PARTITION_SCORES.getPreferredName())
                                .field(TYPE, NESTED)
                                .startObject(PROPERTIES)
                                    .startObject(AnomalyRecord.PARTITION_FIELD_NAME.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject()
                                    .startObject(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject()
                                    .startObject(Bucket.INITIAL_ANOMALY_SCORE.getPreferredName())
                                        .field(TYPE, DOUBLE)
                                    .endObject()
                                    .startObject(AnomalyRecord.ANOMALY_SCORE.getPreferredName())
                                        .field(TYPE, DOUBLE)
                                    .endObject()
                                    .startObject(AnomalyRecord.PROBABILITY.getPreferredName())
                                        .field(TYPE, DOUBLE)
                                    .endObject()
                                .endObject()
                            .endObject()

                            .startObject(Bucket.BUCKET_INFLUENCERS.getPreferredName())
                                .field(TYPE, NESTED)
                                .startObject(PROPERTIES)
                                    .startObject(BucketInfluencer.INFLUENCER_FIELD_NAME.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject()
                                    .startObject(BucketInfluencer.RAW_ANOMALY_SCORE.getPreferredName())
                                        .field(TYPE, DOUBLE)
                                    .endObject()
                                .endObject()
                            .endObject()

                            // per-partition max probabilities mapping
                            .startObject(PerPartitionMaxProbabilities.PER_PARTITION_MAX_PROBABILITIES.getPreferredName())
                                .field(TYPE, NESTED)
                                .startObject(PROPERTIES)
                                    .startObject(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject()
                                    .startObject(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName())
                                        .field(TYPE, DOUBLE)
                                    .endObject()
                                .endObject()
                            .endObject()

                            // Model Debug Output
                            .startObject(ModelDebugOutput.DEBUG_FEATURE.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(ModelDebugOutput.DEBUG_LOWER.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject()
                            .startObject(ModelDebugOutput.DEBUG_UPPER.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject()
                            .startObject(ModelDebugOutput.DEBUG_MEDIAN.getPreferredName())
                                .field(TYPE, DOUBLE)
                            .endObject();

        addAnomalyRecordFieldsToMapping(builder);
        addInfluencerFieldsToMapping(builder);
        addModelSizeStatsFieldsToMapping(builder);

        for (String fieldName : termFieldNames) {
            if (ReservedFieldNames.isValidFieldName(fieldName)) {
                builder.startObject(fieldName).field(TYPE, KEYWORD).endObject();
            }
        }

        // End result properties
        builder.endObject();
        // End result
        builder.endObject();
        // End mapping
        builder.endObject();

        return builder;
    }

    /**
     * AnomalyRecord fields to be added under the 'properties' section of the mapping
     * @param builder Add properties to this builder
     * @return builder
     * @throws IOException On write error
     */
    private static XContentBuilder addAnomalyRecordFieldsToMapping(XContentBuilder builder)
            throws IOException {
        builder.startObject(AnomalyRecord.DETECTOR_INDEX.getPreferredName())
            .field(TYPE, INTEGER)
        .endObject()
        .startObject(AnomalyRecord.SEQUENCE_NUM.getPreferredName())
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
        .startObject(AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName())
            .field(TYPE, DOUBLE)
        .endObject()
        .startObject(AnomalyRecord.INITIAL_NORMALIZED_PROBABILITY.getPreferredName())
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

        return builder;
    }

    private static XContentBuilder addInfluencerFieldsToMapping(XContentBuilder builder) throws IOException {
        builder.startObject(Influencer.INFLUENCER_FIELD_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
        .endObject()
        .startObject(Influencer.INFLUENCER_FIELD_VALUE.getPreferredName())
            .field(TYPE, KEYWORD)
            .field(COPY_TO, ALL_FIELD_VALUES)
        .endObject();

        return builder;
    }

    /**
     * {@link DataCounts} mapping.
     * The type is disabled so {@link DataCounts} aren't searchable and
     * the '_all' field is disabled
     *
     * @return The builder
     * @throws IOException On builder write error
     */
    public static XContentBuilder dataCountsMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(DataCounts.TYPE.getPreferredName())
                        .field(ENABLED, false)
                        .startObject(PROPERTIES)
                            .startObject(Job.ID.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(DataCounts.PROCESSED_RECORD_COUNT.getPreferredName())
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
                            .startObject(DataCounts.EARLIEST_RECORD_TIME.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                            .startObject(DataCounts.LATEST_RECORD_TIME.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }

    /**
     * {@link CategorizerState} mapping.
     * The type is disabled so {@link CategorizerState} is not searchable and
     * the '_all' field is disabled
     *
     * @return The builder
     * @throws IOException On builder write error
     */
    public static XContentBuilder categorizerStateMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(CategorizerState.TYPE)
                        .field(ENABLED, false)
                    .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain Quantiles}.
     * The type is disabled as is the '_all' field as the document isn't meant to be searched.
     * <p>
     * The quantile state string is not searchable (enabled = false) as it could be
     * very large.
     */
    public static XContentBuilder quantilesMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(Quantiles.TYPE.getPreferredName())
                        .field(ENABLED, false)
                    .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain CategoryDefinition}.
     * The '_all' field is disabled as the document isn't meant to be searched.
     *
     * @return The builder
     * @throws IOException On builder error
     */
    public static XContentBuilder categoryDefinitionMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(CategoryDefinition.TYPE.getPreferredName())
                        .startObject(PROPERTIES)
                            .startObject(CategoryDefinition.CATEGORY_ID.getPreferredName())
                                .field(TYPE, LONG)
                            .endObject()
                            .startObject(Job.ID.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(CategoryDefinition.TERMS.getPreferredName())
                                .field(TYPE, TEXT)
                            .endObject()
                            .startObject(CategoryDefinition.REGEX.getPreferredName())
                                .field(TYPE, TEXT)
                            .endObject()
                            .startObject(CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName())
                                .field(TYPE, LONG)
                            .endObject()
                            .startObject(CategoryDefinition.EXAMPLES.getPreferredName())
                                .field(TYPE, TEXT)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain ModelState}.
     * The model state could potentially be huge (over a gigabyte in size)
     * so all analysis by Elasticsearch is disabled.  The only way to
     * retrieve the model state is by knowing the ID of a particular
     * document or by searching for all documents of this type.
     */
    public static XContentBuilder modelStateMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(ModelState.TYPE.getPreferredName())
                        .field(ENABLED, false)
                    .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain ModelSnapshot}.
     * The '_all' field is disabled but the type is searchable
     */
    public static XContentBuilder modelSnapshotMapping() throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                    .startObject(ModelSnapshot.TYPE.getPreferredName())
                        .startObject(PROPERTIES)
                            .startObject(Job.ID.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(ModelSnapshot.TIMESTAMP.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                            // "description" is analyzed so that it has the same
                            // mapping as a user field of the same name - this means
                            // it doesn't have to be a reserved field name
                            .startObject(ModelSnapshot.DESCRIPTION.getPreferredName())
                                .field(TYPE, TEXT)
                            .endObject()
                            .startObject(ModelSnapshot.RESTORE_PRIORITY.getPreferredName())
                                .field(TYPE, LONG)
                            .endObject()
                            .startObject(ModelSnapshot.SNAPSHOT_ID.getPreferredName())
                                .field(TYPE, KEYWORD)
                            .endObject()
                            .startObject(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName())
                                .field(TYPE, INTEGER)
                            .endObject()
                            .startObject(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName())
                                .startObject(PROPERTIES)
                                    .startObject(Job.ID.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject();

        addModelSizeStatsFieldsToMapping(builder);

                         builder.endObject()
                            .endObject()
                            .startObject(Quantiles.TYPE.getPreferredName())
                                .startObject(PROPERTIES)
                                    .startObject(Job.ID.getPreferredName())
                                        .field(TYPE, KEYWORD)
                                    .endObject()
                                    .startObject(Quantiles.TIMESTAMP.getPreferredName())
                                        .field(TYPE, DATE)
                                    .endObject()
                                    .startObject(Quantiles.QUANTILE_STATE.getPreferredName())
                                        .field(TYPE, TEXT)
                                    .endObject()
                                 .endObject()
                            .endObject()
                            .startObject(ModelSnapshot.LATEST_RECORD_TIME.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                            .startObject(ModelSnapshot.LATEST_RESULT_TIME.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        return builder;
    }

    /**
     * {@link ModelSizeStats} fields to be added under the 'properties' section of the mapping
     * @param builder Add properties to this builder
     * @return builder
     * @throws IOException On write error
     */
    private static XContentBuilder addModelSizeStatsFieldsToMapping(XContentBuilder builder) throws IOException {
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

        return builder;
    }

    public static XContentBuilder auditMessageMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(AuditMessage.TYPE.getPreferredName())
                        .startObject(PROPERTIES)
                            .startObject(AuditMessage.TIMESTAMP.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }

    public static XContentBuilder auditActivityMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject(AuditActivity.TYPE.getPreferredName())
                        .startObject(PROPERTIES)
                            .startObject(AuditActivity.TIMESTAMP.getPreferredName())
                                .field(TYPE, DATE)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }
}
