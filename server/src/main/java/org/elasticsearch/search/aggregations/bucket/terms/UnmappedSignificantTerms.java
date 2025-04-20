/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

/**
 * Result of the running the significant terms aggregation on an unmapped field.
 */
public class UnmappedSignificantTerms extends InternalSignificantTerms<UnmappedSignificantTerms, UnmappedSignificantTerms.Bucket> {

    public static final String NAME = "umsigterms";

    /**
     * Concrete type that can't be built because Java needs a concrete type so {@link InternalTerms.Bucket} can have a self type but
     * {@linkplain UnmappedTerms} doesn't ever need to build it because it never returns any buckets.
     */
    protected abstract static class Bucket extends InternalSignificantTerms.Bucket<Bucket> {
        private Bucket(long subsetDf, long supersetDf, InternalAggregations aggregations, DocValueFormat format) {
            super(subsetDf, supersetDf, aggregations, format);
        }
    }

    public UnmappedSignificantTerms(String name, int requiredSize, long minDocCount, Map<String, Object> metadata) {
        super(name, requiredSize, minDocCount, metadata);
    }

    /**
     * Read from a stream.
     */
    public UnmappedSignificantTerms(StreamInput in) throws IOException {
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
        return SignificantStringTerms.NAME;
    }

    @Override
    public UnmappedSignificantTerms create(List<Bucket> buckets) {
        return new UnmappedSignificantTerms(name, requiredSize, minDocCount, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    protected UnmappedSignificantTerms create(long subsetSize, long supersetSize, List<Bucket> buckets) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    Bucket createBucket(long subsetDf, long supersetDf, InternalAggregations aggregations, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new UnmappedSignificantTerms(name, requiredSize, minDocCount, metadata);
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canLeadReduction() {
        return false;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName()).endArray();
        return builder;
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public Iterator<SignificantTerms.Bucket> iterator() {
        return emptyIterator();
    }

    @Override
    public List<Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public SignificantTerms.Bucket getBucketByKey(String term) {
        return null;
    }

    @Override
    protected SignificanceHeuristic getSignificanceHeuristic() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSubsetSize() {
        return 0;
    }

    @Override
    public long getSupersetSize() {
        return 0;
    }
}
