/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link BucketsAggregator} which resolves to the matching parent documents.
 */
public class ChildrenToParentAggregator extends ParentJoinAggregator {

    static final ParseField TYPE_FIELD = new ParseField("type");

    public ChildrenToParentAggregator(String name, AggregatorFactories factories,
            AggregationContext context, Aggregator parent, Query childFilter,
            Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
            long maxOrd, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, childFilter, parentFilter, valuesSource, maxOrd, cardinality, metadata);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalParent(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalParent(name, 0, buildEmptySubAggregations(), metadata());
    }
}
