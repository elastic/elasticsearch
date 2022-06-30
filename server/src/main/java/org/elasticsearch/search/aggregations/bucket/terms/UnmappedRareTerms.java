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
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
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
 * Result of the RareTerms aggregation when the field is unmapped.
 */
public class UnmappedRareTerms extends InternalRareTerms<UnmappedRareTerms, UnmappedRareTerms.Bucket> {
    public static final String NAME = "umrareterms";

    protected abstract static class Bucket extends InternalRareTerms.Bucket<Bucket> {
        private Bucket(long docCount, InternalAggregations aggregations, DocValueFormat formatter) {
            super(docCount, aggregations, formatter);
        }
    }

    UnmappedRareTerms(String name, Map<String, Object> metadata) {
        super(name, LongRareTermsAggregator.ORDER, 0, metadata);
    }

    /**
     * Read from a stream.
     */
    public UnmappedRareTerms(StreamInput in) throws IOException {
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
    public UnmappedRareTerms create(List<UnmappedRareTerms.Bucket> buckets) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    public UnmappedRareTerms.Bucket createBucket(InternalAggregations aggregations, UnmappedRareTerms.Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    UnmappedRareTerms.Bucket createBucket(long docCount, InternalAggregations aggs, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    protected UnmappedRareTerms createWithFilter(String name, List<UnmappedRareTerms.Bucket> buckets, SetBackedScalingCuckooFilter filter) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    public boolean canLeadReduction() {
        return false;
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, Collections.emptyList());
    }

    @Override
    public List<UnmappedRareTerms.Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public UnmappedRareTerms.Bucket getBucketByKey(String term) {
        return null;
    }

    @Override
    protected UnmappedRareTerms.Bucket[] createBucketsArray(int size) {
        return new UnmappedRareTerms.Bucket[size];
    }
}
