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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Result of the RareTerms aggregation when the field is unmapped.
 */
public class UnmappedRareTerms extends InternalTerms<UnmappedRareTerms, UnmappedTerms.Bucket> {
    public static final String NAME = "umrareterms";

    UnmappedRareTerms(String name, List<PipelineAggregator> pipelineAggregators,
                             Map<String, Object> metaData) {
        super(name, LongRareTermsAggregator.ORDER, 0, 0, pipelineAggregators, metaData);
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
    public UnmappedRareTerms create(List<UnmappedTerms.Bucket> buckets) {
        return new UnmappedRareTerms(name, pipelineAggregators(), metaData);
    }

    @Override
    public UnmappedTerms.Bucket createBucket(InternalAggregations aggregations, UnmappedTerms.Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedTerms");
    }

    @Override
    protected UnmappedRareTerms create(String name, List<UnmappedTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        throw new UnsupportedOperationException("not supported for UnmappedTerms");
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        return new UnmappedRareTerms(name, pipelineAggregators(), metaData);
    }

    @Override
    public boolean isMapped() {
        return false;
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, 0, 0, Collections.emptyList());
    }

    @Override
    protected void setDocCountError(long docCountError) {
    }

    @Override
    protected int getShardSize() {
        return 0;
    }

    @Override
    public long getDocCountError() {
        return 0;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return 0;
    }

    @Override
    public List<UnmappedTerms.Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public UnmappedTerms.Bucket getBucketByKey(String term) {
        return null;
    }

    @Override
    protected UnmappedTerms.Bucket[] createBucketsArray(int size) {
        return new UnmappedTerms.Bucket[size];
    }
}
