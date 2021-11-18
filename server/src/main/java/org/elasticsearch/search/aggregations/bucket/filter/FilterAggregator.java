/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Aggregate all docs that match a filter.
 */
public class FilterAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final Supplier<Weight> filter;

    public FilterAggregator(
        String name,
        Supplier<Weight> filter,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.filter = filter;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filter.get().scorerSupplier(ctx));
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bits.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalFilter(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations(), metadata());
    }
}
