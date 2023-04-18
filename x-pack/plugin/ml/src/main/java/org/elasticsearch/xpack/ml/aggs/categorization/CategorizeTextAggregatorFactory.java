/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.IOException;
import java.util.Map;

public class CategorizeTextAggregatorFactory extends AggregatorFactory {

    private final MappedFieldType fieldType;
    private final int similarityThreshold;
    private final CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    public CategorizeTextAggregatorFactory(
        String name,
        String fieldName,
        int similarityThreshold,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        CategorizationAnalyzerConfig categorizationAnalyzerConfig,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.fieldType = context.getFieldType(fieldName);
        this.similarityThreshold = similarityThreshold;
        this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedCategorizationAggregation(
            name,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            similarityThreshold,
            metadata
        );
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (fieldType == null) {
            return createUnmapped(parent, metadata);
        }
        // Most of the text and keyword family of fields use a bespoke TextSearchInfo that doesn't match any
        // of the static final ones created in the TextSearchInfo class definition. KeywordScriptFieldType is
        // the exception that we do want to support, so we need to check for that separately. (It's not a
        // complete disaster if we end up analyzing an inappropriate field, for example if the user has added
        // a new field type via a plugin that also creates a bespoke TextSearchInfo member - it will just get
        // converted to a string and then likely the analyzer won't create any tokens, so the categorizer
        // will see an empty token list.)
        if (fieldType.getTextSearchInfo() == TextSearchInfo.NONE
            || (fieldType.getTextSearchInfo() == TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS
                && fieldType instanceof KeywordScriptFieldType == false)) {
            throw new IllegalArgumentException(
                "categorize_text agg ["
                    + name
                    + "] only works on text and keyword fields. Cannot aggregate field type ["
                    + fieldType.name()
                    + "] via ["
                    + fieldType.getClass().getSimpleName()
                    + "]"
            );
        }
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == CategorizeTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.shardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            // TODO significant text does a 2x here, should we as well?
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();

        return new CategorizeTextAggregator(
            name,
            factories,
            context,
            parent,
            fieldType.name(),
            fieldType,
            bucketCountThresholds,
            similarityThreshold,
            categorizationAnalyzerConfig,
            metadata
        );
    }
}
