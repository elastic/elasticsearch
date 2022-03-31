/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

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
    private final String indexedFieldName;
    private final int maxUniqueTokens;
    private final int maxMatchTokens;
    private final int similarityThreshold;
    private final CategorizationAnalyzerConfig categorizationAnalyzerConfig;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    public CategorizeTextAggregatorFactory(
        String name,
        String fieldName,
        int maxUniqueTokens,
        int maxMatchTokens,
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
        if (fieldType != null) {
            this.indexedFieldName = fieldType.name();
        } else {
            this.indexedFieldName = null;
        }
        this.maxUniqueTokens = maxUniqueTokens;
        this.maxMatchTokens = maxMatchTokens;
        this.similarityThreshold = similarityThreshold;
        this.categorizationAnalyzerConfig = categorizationAnalyzerConfig;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedCategorizationAggregation(
            name,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            maxUniqueTokens,
            maxMatchTokens,
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
        // TODO add support for Keyword && KeywordScriptFieldType
        if (fieldType.getTextSearchInfo() == TextSearchInfo.NONE
            || fieldType.getTextSearchInfo() == TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS) {
            throw new IllegalArgumentException(
                "categorize_text agg ["
                    + name
                    + "] only works on analyzable text fields. Cannot aggregate field type ["
                    + fieldType.name()
                    + "] via ["
                    + fieldType.getClass().getSimpleName()
                    + "]"
            );
        }
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (thresholds.getShardSize() == CategorizeTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            // TODO significant text does a 2x here, should we as well?
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();

        return new CategorizeTextAggregator(
            name,
            factories,
            context,
            parent,
            indexedFieldName,
            fieldType,
            thresholds,
            maxUniqueTokens,
            maxMatchTokens,
            similarityThreshold,
            categorizationAnalyzerConfig,
            metadata
        );
    }

}
