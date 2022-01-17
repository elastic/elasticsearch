/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.missing;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class MissingAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final ValuesSource valuesSource;

    public MissingAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final DocValueBits docsWithValue;
        if (valuesSource != null) {
            docsWithValue = valuesSource.docsWithValue(ctx);
        } else {
            docsWithValue = new DocValueBits() {
                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return false;
                }
            };
        }
        return new LeafBucketCollectorBase(sub, docsWithValue) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (docsWithValue.advanceExact(doc) == false) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalMissing(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMissing(name, 0, buildEmptySubAggregations(), metadata());
    }

}
