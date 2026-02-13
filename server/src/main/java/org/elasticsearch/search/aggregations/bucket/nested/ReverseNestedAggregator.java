/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReverseNestedAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final BitSetProducer parentBitsetProducer;

    public ReverseNestedAggregator(
        String name,
        AggregatorFactories factories,
        NestedObjectMapper objectMapper,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        Query parentFilter;
        if (objectMapper == null) {
            parentFilter = Queries.newNonNestedFilter(context.getIndexSettings().getIndexVersionCreated());
        } else {
            parentFilter = objectMapper.nestedTypeFilter();
        }
        parentBitsetProducer = context.bitsetFilterCache().getBitSetProducer(parentFilter);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        // In ES if parent is deleted, then also the children are deleted, so the child docs this agg receives
        // must belong to parent docs that is alive. For this reason acceptedDocs can be null here.
        final BitSet parentDocs = parentBitsetProducer.getBitSet(aggCtx.getLeafReaderContext());
        if (parentDocs == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final Map<Long, Integer> bucketOrdToLastCollectedParentDoc = new HashMap<>(32);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int childDoc, long bucket) throws IOException {
                // fast forward to retrieve the parentDoc this childDoc belongs to
                final int parentDoc = parentDocs.nextSetBit(childDoc);
                assert childDoc <= parentDoc && parentDoc != DocIdSetIterator.NO_MORE_DOCS;

                Integer lastCollectedParentDoc = bucketOrdToLastCollectedParentDoc.get(bucket);
                if (lastCollectedParentDoc != null) {
                    if (parentDoc > lastCollectedParentDoc) {
                        collectBucket(sub, parentDoc, bucket);
                        bucketOrdToLastCollectedParentDoc.put(bucket, parentDoc);
                    }
                } else {
                    collectBucket(sub, parentDoc, bucket);
                    bucketOrdToLastCollectedParentDoc.put(bucket, parentDoc);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalReverseNested(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalReverseNested(name, 0, buildEmptySubAggregations(), metadata());
    }

}
