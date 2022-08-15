/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Result of the {@link TermsAggregator} when the field is unmapped.
 */
public class UnmappedTerms extends InternalTerms<UnmappedTerms, UnmappedTerms.Bucket> {
    public static final String NAME = "umterms";

    /**
     * Concrete type that can't be built because Java needs a concrete type so {@link InternalTerms.Bucket} can have a self type but
     * {@linkplain UnmappedTerms} doesn't ever need to build it because it never returns any buckets.
     */
    protected abstract static class Bucket extends InternalTerms.Bucket<Bucket> {
        private Bucket(
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat formatter
        ) {
            super(docCount, aggregations, showDocCountError, docCountError, formatter);
        }
    }

    public UnmappedTerms(String name, BucketOrder order, int requiredSize, long minDocCount, Map<String, Object> metadata) {
        super(name, order, order, requiredSize, minDocCount, metadata);
    }

    /**
     * Read from a stream.
     */
    public UnmappedTerms(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getType() {
        return StringTerms.NAME;
    }

    @Override
    public UnmappedTerms create(List<Bucket> buckets) {
        return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedTerms");
    }

    @Override
    protected Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedTerms");
    }

    @Override
    protected UnmappedTerms create(String name, List<Bucket> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount) {
        throw new UnsupportedOperationException("not supported for UnmappedTerms");
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
    }

    @Override
    public boolean isMapped() {
        return false;
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, 0L, 0, Collections.emptyList());
    }

    @Override
    protected void setDocCountError(long docCountError) {}

    @Override
    protected int getShardSize() {
        return 0;
    }

    @Override
    public Long getDocCountError() {
        return 0L;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return 0;
    }

    @Override
    public List<Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public Bucket getBucketByKey(String term) {
        return null;
    }
}
