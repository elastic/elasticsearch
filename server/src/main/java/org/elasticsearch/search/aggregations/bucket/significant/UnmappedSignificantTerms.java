/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

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
        private Bucket(BytesRef term, long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations,
                       DocValueFormat format) {
            super(subsetDf, subsetSize, supersetDf, supersetSize, aggregations, format);
        }
    }

    public UnmappedSignificantTerms(String name, int requiredSize, long minDocCount, List<PipelineAggregator> pipelineAggregators,
                                    Map<String, Object> metaData) {
        super(name, requiredSize, minDocCount, pipelineAggregators, metaData);
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
        return new UnmappedSignificantTerms(name, requiredSize, minDocCount, pipelineAggregators(), metaData);
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
    Bucket createBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize,
                        InternalAggregations aggregations, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedSignificantTerms");
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        return new UnmappedSignificantTerms(name, requiredSize, minDocCount, pipelineAggregators(), metaData);
    }

    @Override
    public boolean isMapped() {
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
    protected long getSubsetSize() {
        return 0;
    }

    @Override
    protected long getSupersetSize() {
        return 0;
    }
}
