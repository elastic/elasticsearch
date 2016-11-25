/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.prelert.job.CategorizerState;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.ModelState;
import org.elasticsearch.xpack.prelert.job.audit.AuditActivity;
import org.elasticsearch.xpack.prelert.job.audit.AuditMessage;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AnomalyCause;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.Influence;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;
import org.elasticsearch.xpack.prelert.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.prelert.job.usage.Usage;

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
 * set in the index settings during index creation.  Then the _all field has its
 * analyzer set to "whitespace" by these mappings, so that _all gets tokenised
 * using whitespace.
 */
public class ElasticsearchMappings {
    /**
     * String constants used in mappings
     */
    static final String INDEX = "index";
    static final String NO = "false";
    static final String ALL = "_all";
    static final String ENABLED = "enabled";
    static final String ANALYZER = "analyzer";
    static final String WHITESPACE = "whitespace";
    static final String INCLUDE_IN_ALL = "include_in_all";
    static final String NESTED = "nested";
    static final String COPY_TO = "copy_to";
    static final String PROPERTIES = "properties";
    static final String TYPE = "type";
    static final String DYNAMIC = "dynamic";

    /**
     * Name of the field used to store the timestamp in Elasticsearch.
     * Note the field name is different to {@link org.elasticsearch.xpack.prelert.job.results.Bucket#TIMESTAMP} used by the
     * API Bucket Resource, and is chosen for consistency with the default field name used by
     * Logstash and Kibana.
     */
    static final String ES_TIMESTAMP = "timestamp";

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


    public static XContentBuilder dataCountsMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(DataCounts.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
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
     * Create the Elasticsearch mapping for {@linkplain org.elasticsearch.xpack.prelert.job.results.Bucket}.
     * The '_all' field is disabled as the document isn't meant to be searched.
     */
    public static XContentBuilder bucketMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(Bucket.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ES_TIMESTAMP)
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
                .startObject(Bucket.BUCKET_INFLUENCERS.getPreferredName())
                .field(TYPE, NESTED)
                .startObject(PROPERTIES)
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
                .endObject()
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
                .startObject(AnomalyRecord.ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE)
                .endObject()
                .startObject(AnomalyRecord.PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain org.elasticsearch.xpack.prelert.job.results.BucketInfluencer}.
     */
    public static XContentBuilder bucketInfluencerMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(BucketInfluencer.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(Bucket.IS_INTERIM.getPreferredName())
                .field(TYPE, BOOLEAN)
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
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * Partition normalized scores. There is one per bucket
     * so the timestamp is sufficient to uniquely identify
     * the document per bucket per job
     * <p>
     * Partition field values and scores are nested objects.
     */
    public static XContentBuilder bucketPartitionMaxNormalizedScores() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(ReservedFieldNames.PARTITION_NORMALIZED_PROB_TYPE)
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(ReservedFieldNames.PARTITION_NORMALIZED_PROBS)
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
                .endObject()
                .endObject()
                .endObject();
    }

    public static XContentBuilder categorizerStateMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(CategorizerState.TYPE)
                .field(ENABLED, false)
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .endObject()
                .endObject();
    }

    public static XContentBuilder categoryDefinitionMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(CategoryDefinition.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(CategoryDefinition.CATEGORY_ID.getPreferredName())
                .field(TYPE, LONG)
                .endObject()
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(CategoryDefinition.TERMS.getPreferredName())
                .field(TYPE, TEXT).field(INDEX, NO)
                .endObject()
                .startObject(CategoryDefinition.REGEX.getPreferredName())
                .field(TYPE, TEXT).field(INDEX, NO)
                .endObject()
                .startObject(CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName())
                .field(TYPE, LONG)
                .endObject()
                .startObject(CategoryDefinition.EXAMPLES.getPreferredName())
                .field(TYPE, TEXT).field(INDEX, NO)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * @param termFieldNames Optionally, other field names to include in the
     *                       mappings.  Pass <code>null</code> if not required.
     */
    public static XContentBuilder recordMapping(Collection<String> termFieldNames) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject(AnomalyRecord.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.DETECTOR_INDEX.getPreferredName())
                .field(TYPE, INTEGER).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.ACTUAL.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.TYPICAL.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.FUNCTION.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.FUNCTION_DESCRIPTION.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.BY_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.BY_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyRecord.FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.PARTITION_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyRecord.OVER_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.OVER_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyRecord.CAUSES.getPreferredName())
                .field(TYPE, NESTED)
                .startObject(PROPERTIES)
                .startObject(AnomalyCause.ACTUAL.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.TYPICAL.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.FUNCTION.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.FUNCTION_DESCRIPTION.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.BY_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.BY_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.CORRELATED_BY_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.PARTITION_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.PARTITION_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(AnomalyCause.OVER_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyCause.OVER_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .endObject()
                .endObject()
                .startObject(AnomalyRecord.ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.INITIAL_NORMALIZED_PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.IS_INTERIM.getPreferredName())
                .field(TYPE, BOOLEAN).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(AnomalyRecord.INFLUENCERS.getPreferredName())
                /* Array of influences */
                .field(TYPE, NESTED)
                .startObject(PROPERTIES)
                .startObject(Influence.INFLUENCER_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influence.INFLUENCER_FIELD_VALUES.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .endObject()
                .endObject();

        if (termFieldNames != null) {
            ElasticsearchDotNotationReverser reverser = new ElasticsearchDotNotationReverser();
            for (String fieldName : termFieldNames) {
                reverser.add(fieldName, "");
            }
            for (Map.Entry<String, Object> entry : reverser.getMappingsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        return builder
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain Quantiles}.
     * The '_all' field is disabled as the document isn't meant to be searched.
     * <p>
     * The quantile state string is not searchable (index = 'no') as it could be
     * very large.
     */
    public static XContentBuilder quantilesMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(Quantiles.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(Quantiles.QUANTILE_STATE.getPreferredName())
                .field(TYPE, TEXT).field(INDEX, NO)
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
                .startObject(ModelState.TYPE)
                .field(ENABLED, false)
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
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
    public static XContentBuilder modelSnapshotMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(ModelSnapshot.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ES_TIMESTAMP)
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
                .startObject(ModelSizeStats.TYPE.getPreferredName())
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName())
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
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(ModelSizeStats.LOG_TIME_FIELD.getPreferredName())
                .field(TYPE, DATE)
                .endObject()
                .endObject()
                .endObject()
                .startObject(Quantiles.TYPE.getPreferredName())
                .startObject(PROPERTIES)
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(Quantiles.QUANTILE_STATE.getPreferredName())
                .field(TYPE, TEXT).field(INDEX, NO)
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
    }

    /**
     * Create the Elasticsearch mapping for {@linkplain ModelSizeStats}.
     */
    public static XContentBuilder modelSizeStatsMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(ModelSizeStats.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName())
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
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(ModelSizeStats.LOG_TIME_FIELD.getPreferredName())
                .field(TYPE, DATE)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * Mapping for model debug output
     *
     * @param termFieldNames Optionally, other field names to include in the
     *                       mappings.  Pass <code>null</code> if not required.
     */
    public static XContentBuilder modelDebugOutputMapping(Collection<String> termFieldNames) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject(ModelDebugOutput.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ModelDebugOutput.PARTITION_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ModelDebugOutput.OVER_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ModelDebugOutput.BY_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(ModelDebugOutput.DEBUG_FEATURE.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ModelDebugOutput.DEBUG_LOWER.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ModelDebugOutput.DEBUG_UPPER.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ModelDebugOutput.DEBUG_MEDIAN.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ModelDebugOutput.ACTUAL.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject();

        if (termFieldNames != null) {
            ElasticsearchDotNotationReverser reverser = new ElasticsearchDotNotationReverser();
            for (String fieldName : termFieldNames) {
                reverser.add(fieldName, "");
            }
            for (Map.Entry<String, Object> entry : reverser.getMappingsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        return builder
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * Influence results mapping
     *
     * @param influencerFieldNames Optionally, other field names to include in the
     *                             mappings.  Pass <code>null</code> if not required.
     */
    public static XContentBuilder influencerMapping(Collection<String> influencerFieldNames) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject(Influencer.TYPE.getPreferredName())
                .startObject(ALL)
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(Job.ID.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influencer.PROBABILITY.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influencer.INITIAL_ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influencer.ANOMALY_SCORE.getPreferredName())
                .field(TYPE, DOUBLE).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influencer.INFLUENCER_FIELD_NAME.getPreferredName())
                .field(TYPE, KEYWORD).field(INCLUDE_IN_ALL, false)
                .endObject()
                .startObject(Influencer.INFLUENCER_FIELD_VALUE.getPreferredName())
                .field(TYPE, KEYWORD)
                .endObject()
                .startObject(Bucket.IS_INTERIM.getPreferredName())
                .field(TYPE, BOOLEAN).field(INCLUDE_IN_ALL, false)
                .endObject();

        if (influencerFieldNames != null) {
            ElasticsearchDotNotationReverser reverser = new ElasticsearchDotNotationReverser();
            for (String fieldName : influencerFieldNames) {
                reverser.add(fieldName, "");
            }
            for (Map.Entry<String, Object> entry : reverser.getMappingsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        return builder
                .endObject()
                .endObject()
                .endObject();
    }

    /**
     * The Elasticsearch mappings for the usage documents
     */
    public static XContentBuilder usageMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(Usage.TYPE)
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .startObject(Usage.INPUT_BYTES)
                .field(TYPE, LONG)
                .endObject()
                .startObject(Usage.INPUT_FIELD_COUNT)
                .field(TYPE, LONG)
                .endObject()
                .startObject(Usage.INPUT_RECORD_COUNT)
                .field(TYPE, LONG)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    public static XContentBuilder auditMessageMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(AuditMessage.TYPE.getPreferredName())
                .startObject(PROPERTIES)
                .startObject(ES_TIMESTAMP)
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
                .startObject(ES_TIMESTAMP)
                .field(TYPE, DATE)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    public static XContentBuilder processingTimeMapping() throws IOException {
        return jsonBuilder()
                .startObject()
                .startObject(ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE)
                .startObject(ALL)
                .field(ENABLED, false)
                // analyzer must be specified even though _all is disabled
                // because all types in the same index must have the same
                // analyzer for a given field
                .field(ANALYZER, WHITESPACE)
                .endObject()
                .startObject(PROPERTIES)
                .startObject(ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS)
                .field(TYPE, DOUBLE)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }
}
