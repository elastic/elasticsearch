/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.CollectedAggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.util.List;
import java.util.Map;

public class CollectedDateHistogram extends CollectedAggregator {

    private final LongArray docCounts;

    private final LongKeyedBucketOrds bucketOrds;

    public CollectedDateHistogram(String name, Map<String, Object> metadata, LongArray docCounts, LongKeyedBucketOrds bucketOrds) {
        super(name, metadata);
        this.bucketOrds = bucketOrds;
        this.docCounts = docCounts;
    }

    @Override
    public void close() {
        Releasables.close(docCounts, bucketOrds);
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public CollectedAggregator reduceBuckets(
        CollectedAggregator reductionTarget,
        List<MultiBucketsAggregation.Bucket> buckets,
        AggregationReduceContext reduceContext
    ) {
        return null;
    }

    @Override
    public CollectedAggregator reduceTopLevel(List<CollectedAggregator> aggregators, AggregationReduceContext reduceContext) {
        CollectedDateHistogram reduced = new CollectedDateHistogram(
            name,
            metadata,
            reduceContext.bigArrays().newLongArray(1),
            // TODO: We can probably make a better size estimate here
            LongKeyedBucketOrds.build(reduceContext.bigArrays(), CardinalityUpperBound.MANY)
        );
        // Build the priority queue of key iterators
        final PriorityQueue<IteratorAndCurrent<InternalDateHistogram.Bucket>> pq = new PriorityQueue<>(aggregators.size()) {
            @Override
            protected boolean lessThan(
                IteratorAndCurrent<InternalDateHistogram.Bucket> a,
                IteratorAndCurrent<InternalDateHistogram.Bucket> b
            ) {
                return a.current().key < b.current().key;
            }
        };
        for (CollectedAggregator agg : aggregators) {
            // Get the iterators and stuff 'em in the queue
        }
        return reduced;
    }

    @Override
    public IteratorAndCurrent<KeyComparable<?>> getKeysIteratorForOwningBucketOrd(long owningBucketOrd) {
        return null;
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public CollectedAggregator finalizeSampling(SamplingContext samplingContext) {
        return null;
    }

    @Override
    public CollectedAggregator reducePipelines(
        CollectedAggregator agg,
        AggregationReduceContext context,
        PipelineAggregator.PipelineTree pipelines
    ) {
        return null;
    }

    @Override
    public InternalAggregation convertToLegacy(long bucketOrdinal) {
        return null;
    }
}
