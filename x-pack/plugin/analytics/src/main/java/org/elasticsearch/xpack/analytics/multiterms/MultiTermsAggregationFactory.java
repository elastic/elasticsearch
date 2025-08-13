/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MultiTermsAggregationFactory extends AggregatorFactory {

    protected final List<ValuesSourceConfig> configs;
    protected final List<DocValueFormat> formats;
    private final boolean showTermDocCountError;
    private final BucketOrder order;
    private final Aggregator.SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    public MultiTermsAggregationFactory(
        String name,
        List<ValuesSourceConfig> configs,
        List<DocValueFormat> formats,
        boolean showTermDocCountError,
        BucketOrder order,
        Aggregator.SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
        this.formats = formats;
        this.showTermDocCountError = showTermDocCountError;
        this.order = order;
        this.collectMode = collectMode;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && thresholds.getShardSize() == MultiTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.shardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();
        return new MultiTermsAggregator(
            name,
            factories,
            context,
            parent,
            configs,
            formats,
            showTermDocCountError,
            order,
            collectMode,
            thresholds,
            cardinality,
            metadata
        );
    }
}
