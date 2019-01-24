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
import org.elasticsearch.common.util.ExactBloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Result of the RareTerms aggregation when the field is some kind of whole number like a integer, long, or a date.
 */
public class LongRareTerms extends InternalMappedRareTerms<LongRareTerms, LongTerms.Bucket> {
    public static final String NAME = "lrareterms";

    LongRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                         Map<String, Object> metaData, DocValueFormat format,
                         List<LongTerms.Bucket> buckets, long maxDocCount, ExactBloomFilter bloom) {
        super(name, order, pipelineAggregators, metaData, format, buckets, maxDocCount, bloom);
    }

    /**
     * Read from a stream.
     */
    public LongRareTerms(StreamInput in) throws IOException {
        super(in, LongTerms.Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongRareTerms create(List<LongTerms.Bucket> buckets) {
        return new LongRareTerms(name, order, pipelineAggregators(), metaData, format,
            buckets, maxDocCount, bloom);
    }

    @Override
    public LongTerms.Bucket createBucket(InternalAggregations aggregations, LongTerms.Bucket prototype) {
        return new LongTerms.Bucket(prototype.term, prototype.getDocCount(), aggregations, prototype.showDocCountError,
            prototype.docCountError, prototype.format);
    }

    @Override
    protected LongRareTerms create(String name, List<LongTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new LongRareTerms(name, order, pipelineAggregators(), getMetaData(), format,
            buckets, maxDocCount, bloom);
    }

    @Override
    protected LongTerms.Bucket[] createBucketsArray(int size) {
        return new LongTerms.Bucket[size];
    }

    @Override
    public boolean containsTerm(ExactBloomFilter bloom, LongTerms.Bucket bucket) {
        return bloom.mightContain((long) bucket.getKey());
    }

    @Override
    public void addToBloom(ExactBloomFilter bloom, LongTerms.Bucket bucket) {
        bloom.put((long) bucket.getKey());
    }
}
