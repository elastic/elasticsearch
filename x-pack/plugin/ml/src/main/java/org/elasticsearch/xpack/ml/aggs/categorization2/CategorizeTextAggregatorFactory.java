/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization2;

import org.elasticsearch.index.mapper.MappedFieldType;
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
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == CategorizeTextAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
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
