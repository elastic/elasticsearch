/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
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

public class CategorizeTextAggregationBuilder extends AbstractAggregationBuilder<CategorizeTextAggregationBuilder> {

    static final TermsAggregator.ConstantBucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
        new TermsAggregator.ConstantBucketCountThresholds(1, 0, 10, -1);

    public static final String NAME = "categorize_text";

    // In 8.3 the algorithm used by this aggregation was completely changed.
    // Prior to 8.3 the Drain algorithm was used. From 8.3 the same algorithm
    // we use in our C++ categorization code was used. As a result of this
    // the aggregation will not perform well in mixed version clusters where
    // some nodes are pre-8.3 and others are newer, so we throw an error in
    // this situation. The aggregation was experimental at the time this change
    // was made, so this is acceptable.
    public static final TransportVersion ALGORITHM_CHANGED_VERSION = TransportVersion.V_8_3_0;

    static final ParseField FIELD_NAME = new ParseField("field");
    static final ParseField SIMILARITY_THRESHOLD = new ParseField("similarity_threshold");
    // The next two are unused, but accepted and ignored to avoid breaking client code
    static final ParseField MAX_UNIQUE_TOKENS = new ParseField("max_unique_tokens").withAllDeprecated();
    static final ParseField MAX_MATCHED_TOKENS = new ParseField("max_matched_tokens").withAllDeprecated();
    static final ParseField CATEGORIZATION_FILTERS = new ParseField("categorization_filters");
    static final ParseField CATEGORIZATION_ANALYZER = new ParseField("categorization_analyzer");

    public static final ObjectParser<CategorizeTextAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        CategorizeTextAggregationBuilder.NAME,
        CategorizeTextAggregationBuilder::new
    );
    static {
        PARSER.declareString(CategorizeTextAggregationBuilder::setFieldName, FIELD_NAME);
        PARSER.declareInt(CategorizeTextAggregationBuilder::setSimilarityThreshold, SIMILARITY_THRESHOLD);
        // The next two are unused, but accepted and ignored to avoid breaking client code
        PARSER.declareInt((p, c) -> {}, MAX_UNIQUE_TOKENS);
        PARSER.declareInt((p, c) -> {}, MAX_MATCHED_TOKENS);
        PARSER.declareField(
            CategorizeTextAggregationBuilder::setCategorizationAnalyzerConfig,
            (p, c) -> CategorizationAnalyzerConfig.buildFromXContentFragment(p, false),
            CATEGORIZATION_ANALYZER,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareStringArray(CategorizeTextAggregationBuilder::setCategorizationFilters, CATEGORIZATION_FILTERS);
        PARSER.declareInt(CategorizeTextAggregationBuilder::shardSize, TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME);
        PARSER.declareLong(CategorizeTextAggregationBuilder::minDocCount, TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareLong(CategorizeTextAggregationBuilder::shardMinDocCount, TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareInt(CategorizeTextAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);
    }

    public static CategorizeTextAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CategorizeTextAggregationBuilder(aggregationName), null);
    }

    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS
    );
    private CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private String fieldName;
    // Default of 70% matches the C++ code
    private int similarityThreshold = 70;

    private CategorizeTextAggregationBuilder(String name) {
        super(name);
    }

    public CategorizeTextAggregationBuilder(String name, String fieldName) {
        super(name);
        this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    public String getFieldName() {
        return fieldName;
    }

    public CategorizeTextAggregationBuilder setFieldName(String fieldName) {
        this.fieldName = ExceptionsHelper.requireNonNull(fieldName, FIELD_NAME);
        return this;
    }

    public CategorizeTextAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (in.getTransportVersion().before(ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchException(
                "["
                    + NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + ALGORITHM_CHANGED_VERSION
                    + "] or higher and others have a version before this"
            );
        }
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(in);
        this.fieldName = in.readString();
        this.similarityThreshold = in.readVInt();
        this.categorizationAnalyzerConfig = in.readOptionalWriteable(CategorizationAnalyzerConfig::new);
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public CategorizeTextAggregationBuilder setSimilarityThreshold(int similarityThreshold) {
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

    public CategorizeTextAggregationBuilder setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig categorizationAnalyzerConfig) {
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

    public CategorizeTextAggregationBuilder setCategorizationFilters(List<String> categorizationFilters) {
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
    public CategorizeTextAggregationBuilder size(int size) {
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
    public CategorizeTextAggregationBuilder shardSize(int shardSize) {
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
    public CategorizeTextAggregationBuilder minDocCount(long minDocCount) {
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
    public CategorizeTextAggregationBuilder shardMinDocCount(long shardMinDocCount) {
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

    protected CategorizeTextAggregationBuilder(
        CategorizeTextAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(clone.bucketCountThresholds);
        this.fieldName = clone.fieldName;
        this.similarityThreshold = clone.similarityThreshold;
        this.categorizationAnalyzerConfig = clone.categorizationAnalyzerConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (out.getTransportVersion().before(ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchException(
                "["
                    + NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + ALGORITHM_CHANGED_VERSION
                    + "] or higher and others have a version before this"
            );
        }
        bucketCountThresholds.writeTo(out);
        out.writeString(fieldName);
        out.writeVInt(similarityThreshold);
        out.writeOptionalWriteable(categorizationAnalyzerConfig);
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        return new CategorizeTextAggregatorFactory(
            name,
            fieldName,
            similarityThreshold,
            bucketCountThresholds,
            categorizationAnalyzerConfig,
            context,
            parent,
            subfactoriesBuilder,
            metadata
        );
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
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CategorizeTextAggregationBuilder(this, factoriesBuilder, metadata);
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
