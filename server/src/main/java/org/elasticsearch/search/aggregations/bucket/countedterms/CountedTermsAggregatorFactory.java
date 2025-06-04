/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.countedterms;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class CountedTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private final BucketOrder order = BucketOrder.count(false);
    private final CountedTermsAggregatorSupplier supplier;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            CountedTermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD),
            CountedTermsAggregatorFactory.bytesSupplier(),
            true
        );
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    static CountedTermsAggregatorSupplier bytesSupplier() {
        return (name, factories, valuesSourceConfig, order, bucketCountThresholds, context, parent, cardinality, metadata) -> {

            assert valuesSourceConfig.getValuesSource() instanceof ValuesSource.Bytes.WithOrdinals;
            ValuesSource.Bytes.WithOrdinals ordinalsValuesSource = (ValuesSource.Bytes.WithOrdinals) valuesSourceConfig.getValuesSource();

            return new CountedTermsAggregator(
                name,
                factories,
                ordinalsValuesSource,
                order,
                valuesSourceConfig.format(),
                bucketCountThresholds,
                context,
                parent,
                cardinality,
                metadata
            );
        };
    }

    CountedTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        CountedTermsAggregatorSupplier supplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.bucketCountThresholds = bucketCountThresholds;
        this.supplier = supplier;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedTerms(
            name,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
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
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        // If min_doc_count and shard_min_doc_count is provided, we do not support them being larger than 1
        // This is because we cannot be sure about their relative scale when sampled
        if (getSamplingContext().map(SamplingContext::isSampled).orElse(false)) {
            if (bucketCountThresholds.getMinDocCount() > 1 || bucketCountThresholds.getShardMinDocCount() > 1) {
                throw new ElasticsearchStatusException(
                    "aggregation [{}] is within a sampling context; "
                        + "min_doc_count, provided [{}], and min_shard_doc_count, provided [{}], cannot be greater than 1",
                    RestStatus.BAD_REQUEST,
                    name(),
                    bucketCountThresholds.getMinDocCount(),
                    bucketCountThresholds.getShardMinDocCount()
                );
            }
        }
        bucketCountThresholds.ensureValidity();

        return supplier.build(name, factories, config, order, bucketCountThresholds, context, parent, cardinality, metadata);
    }
}
