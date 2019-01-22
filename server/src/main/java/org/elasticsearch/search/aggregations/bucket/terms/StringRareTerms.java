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
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class StringRareTerms extends InternalMappedRareTerms<StringRareTerms, StringTerms.Bucket> {
    public static final String NAME = "srareterms";

    public StringRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                           Map<String, Object> metaData, DocValueFormat format,
                           List<StringTerms.Bucket> buckets, long maxDocCount, BloomFilter bloom) {
        super(name, order, pipelineAggregators, metaData, format, buckets, maxDocCount, bloom);
    }

    /**
     * Read from a stream.
     */
    public StringRareTerms(StreamInput in) throws IOException {
        super(in, StringTerms.Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public StringRareTerms create(List<StringTerms.Bucket> buckets) {
        return new StringRareTerms(name, order, pipelineAggregators(), metaData, format, buckets, maxDocCount, bloom);
    }

    @Override
    public StringTerms.Bucket createBucket(InternalAggregations aggregations, StringTerms.Bucket prototype) {
        return new StringTerms.Bucket(prototype.termBytes, prototype.getDocCount(), aggregations, false,
            prototype.docCountError, prototype.format);
    }

    @Override
    protected StringRareTerms create(String name, List<StringTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new StringRareTerms(name, order, pipelineAggregators(), metaData, format,
            buckets, maxDocCount, bloom);
    }

    @Override
    protected StringTerms.Bucket[] createBucketsArray(int size) {
        return new StringTerms.Bucket[size];
    }

    @Override
    public boolean containsTerm(BloomFilter bloom, StringTerms.Bucket bucket) {
        return bloom.mightContain(bucket.termBytes);
    }

    @Override
    public void addToBloom(BloomFilter bloom, StringTerms.Bucket bucket) {
        bloom.put(bucket.termBytes);
    }
}
