/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.aggs.categorization;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME;
import static org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig.Builder.isValidRegex;

public abstract class AbstractCategorizeTextAggregationBuilder<AB extends AbstractAggregationBuilder<AB>> extends
    AbstractAggregationBuilder<AB> {

    public static final TermsAggregator.ConstantBucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
        new TermsAggregator.ConstantBucketCountThresholds(1, 0, 10, -1);

    public static final String NAME = "categorize_text";

    // In 8.3 the algorithm used by this aggregation was completely changed.
    // Prior to 8.3 the Drain algorithm was used. From 8.3 the same algorithm
    // we use in our C++ categorization code was used. As a result of this
    // the aggregation will not perform well in mixed version clusters where
    // some nodes are pre-8.3 and others are newer, so we throw an error in
    // this situation. The aggregation was experimental at the time this change
    // was made, so this is acceptabl @Override
    // public boolean supportsSampling() {
    // return true;
    // }
    //
    // public String getFieldName() {
    // return fieldName;
    // }
    //
    // public AbstractCategorizeTextAggregationBuilder<AB> setFieldName(String fieldName) {
    // this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
    // return this;
    // }e.
    public static final TransportVersion ALGORITHM_CHANGED_VERSION = TransportVersions.V_8_3_0;

    protected static final ParseField FIELD_NAME = new ParseField("field");
    protected static final ParseField SIMILARITY_THRESHOLD = new ParseField("similarity_threshold");
    // The next two are unused, but accepted and ignored to avoid breaking client code
    protected static final ParseField MAX_UNIQUE_TOKENS = new ParseField("max_unique_tokens").withAllDeprecated();
    protected static final ParseField MAX_MATCHED_TOKENS = new ParseField("max_matched_tokens").withAllDeprecated();
    protected static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");
    protected static final ParseField CATEGORIZATION_ANALYZER = new ParseField("categorization_analyzer");

    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS
    );
    private CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private String fieldName;
    // Default of 70% matches the C++ code
    private int similarityThreshold = 70;

    protected AbstractCategorizeTextAggregationBuilder(String name) {
        super(name);
    }

    public AbstractCategorizeTextAggregationBuilder(String name, String fieldName) {
        super(name);
        this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
    }

    protected AbstractCategorizeTextAggregationBuilder(
        AbstractCategorizeTextAggregationBuilder<AB> clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(clone.bucketCountThresholds);
        this.fieldName = clone.fieldName;
        this.similarityThreshold = clone.similarityThreshold;
        this.categorizationAnalyzerConfig = clone.categorizationAnalyzerConfig;
    }

    public AbstractCategorizeTextAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (in.getTransportVersion().before(ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchStatusException(
                "["
                    + NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                    + "] or higher and others have a version before this",
                RestStatus.BAD_REQUEST
            );
        }
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(in);
        this.fieldName = in.readString();
        this.similarityThreshold = in.readVInt();
        this.categorizationAnalyzerConfig = in.readOptionalWriteable(CategorizationAnalyzerConfig::new);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    public String getFieldName() {
        return fieldName;
    }

    public AbstractCategorizeTextAggregationBuilder<AB> setFieldName(String fieldName) {
        this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
        return this;
    }

    public int getSimilarityThreshold() {
        return similarityThreshold;
    }

    public AbstractCategorizeTextAggregationBuilder<AB> setSimilarityThreshold(int similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
        if (similarityThreshold < 1 || similarityThreshold > 100) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be in the range [1, 100]. Found [{}] in [{}]",
                SIMILARITY_THRESHOLD.getPreferredName(),
                similarityThreshold,
                name
            );
        }
        return this;
    }

    public AbstractCategorizeTextAggregationBuilder<AB> setCategorizationAnalyzerConfig(
        CategorizationAnalyzerConfig categorizationAnalyzerConfig
    ) {
        if (this.categorizationAnalyzerConfig != null) {
            throw ExceptionsHelper.badRequestException(
                "[{}] cannot be used with [{}] - instead specify them as pattern_replace char_filters in the analyzer",
                CATEGORIZATION_FILTERS.getPreferredName(),
                CATEGORIZATION_ANALYZER.getPreferredName()
            );
        }
        this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
        return this;
    }

    public AbstractCategorizeTextAggregationBuilder<AB> setCategorizationFilters(List<String> categorizationFilters) {
        if (categorizationFilters == null || categorizationFilters.isEmpty()) {
            return this;
        }
        if (categorizationAnalyzerConfig != null) {
            throw ExceptionsHelper.badRequestException(
                "[{}] cannot be used with [{}] - instead specify them as pattern_replace char_filters in the analyzer",
                CATEGORIZATION_FILTERS.getPreferredName(),
                CATEGORIZATION_ANALYZER.getPreferredName()
            );
        }
        if (categorizationFilters.stream().distinct().count() != categorizationFilters.size()) {
            throw ExceptionsHelper.badRequestException(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_DUPLICATES);
        }
        if (categorizationFilters.stream().anyMatch(String::isEmpty)) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_EMPTY));
        }
        for (String filter : categorizationFilters) {
            if (isValidRegex(filter) == false) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_INVALID_REGEX, filter)
                );
            }
        }
        this.categorizationAnalyzerConfig = CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(categorizationFilters);
        return this;
    }

    /**
     * @param size indicating how many buckets should be returned
     */
    public AbstractCategorizeTextAggregationBuilder<AB> size(int size) {
        if (size <= 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than 0. Found [{}] in [{}]",
                REQUIRED_SIZE_FIELD_NAME.getPreferredName(),
                size,
                name
            );
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * @param shardSize - indicating the number of buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public AbstractCategorizeTextAggregationBuilder<AB> shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than 0. Found [{}] in [{}]",
                SHARD_SIZE_FIELD_NAME.getPreferredName(),
                shardSize,
                name
            );
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * @param minDocCount the minimum document count a text category should have in order to appear in
     * the response.
     */
    public AbstractCategorizeTextAggregationBuilder<AB> minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than or equal to 0. Found [{}] in [{}]",
                MIN_DOC_COUNT_FIELD_NAME.getPreferredName(),
                minDocCount,
                name
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * @param shardMinDocCount the minimum document count a text category should have on the shard in order to
     * appear in the response.
     */
    public AbstractCategorizeTextAggregationBuilder<AB> shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than or equal to 0. Found [{}] in [{}]",
                SHARD_MIN_DOC_COUNT_FIELD_NAME.getPreferredName(),
                shardMinDocCount,
                name
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return this.bucketCountThresholds;
    }

    protected CategorizationAnalyzerConfig getCategorizationAnalyzerConfig() {
        return categorizationAnalyzerConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (out.getTransportVersion().before(ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchStatusException(
                "["
                    + NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                    + "] or higher and others have a version before this",
                RestStatus.BAD_REQUEST
            );
        }
        bucketCountThresholds.writeTo(out);
        out.writeString(fieldName);
        out.writeVInt(similarityThreshold);
        out.writeOptionalWriteable(categorizationAnalyzerConfig);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        bucketCountThresholds.toXContent(builder, params);
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        builder.field(SIMILARITY_THRESHOLD.getPreferredName(), similarityThreshold);
        if (categorizationAnalyzerConfig != null) {
            categorizationAnalyzerConfig.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // This isn't strictly true, as the categorize_text aggregation has existed since 7.16.
        // However, the implementation completely changed in 8.3, so it's best that if the
        // coordinating node is on 8.3 or above then it should refuse to use this aggregation
        // until the older nodes are upgraded.
        return ALGORITHM_CHANGED_VERSION;
    }
}
