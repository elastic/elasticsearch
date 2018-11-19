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

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Result of the RareTerms aggregation when the field is some kind of decimal number like a float, double, or distance.
 */
public class DoubleRareTerms extends InternalMappedRareTerms<DoubleRareTerms, DoubleTerms.Bucket> {
    public static final String NAME = "drareterms";

    DoubleRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                           Map<String, Object> metaData, DocValueFormat format,
                           List<DoubleTerms.Bucket> buckets, long maxDocCount, BloomFilter bloom) {
        super(name, order, pipelineAggregators, metaData, format, buckets, maxDocCount, bloom);
    }

    /**
     * Read from a stream.
     */
    public DoubleRareTerms(StreamInput in) throws IOException {
        super(in, DoubleTerms.Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public DoubleRareTerms create(List<DoubleTerms.Bucket> buckets) {
        return new DoubleRareTerms(name, order, pipelineAggregators(), metaData, format, buckets, maxDocCount, bloom);
    }

    @Override
    public DoubleTerms.Bucket createBucket(InternalAggregations aggregations, DoubleTerms.Bucket prototype) {
        return new DoubleTerms.Bucket((double)prototype.getKey(), prototype.docCount, aggregations,
            prototype.showDocCountError, prototype.docCountError, prototype.format);
    }

    @Override
    protected DoubleRareTerms create(String name, List<DoubleTerms.Bucket> buckets, long docCountError, long otherDocCount) {
        return new DoubleRareTerms(name, order, pipelineAggregators(), getMetaData(), format,
            buckets, maxDocCount, bloom);
    }

    @Override
    protected DoubleTerms.Bucket[] createBucketsArray(int size) {
        return new DoubleTerms.Bucket[size];
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        boolean promoteToDouble = false;
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof LongRareTerms && ((LongRareTerms) agg).format == DocValueFormat.RAW) {
                /*
                 * this agg mixes longs and doubles, we must promote longs to doubles to make the internal aggs
                 * compatible
                 */
                promoteToDouble = true;
                break;
            }
        }
        if (promoteToDouble == false) {
            return super.doReduce(aggregations, reduceContext);
        }
        List<InternalAggregation> newAggs = new ArrayList<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof LongRareTerms) {
                DoubleRareTerms dTerms = LongRareTerms.convertLongRareTermsToDouble((LongRareTerms) agg, format);
                newAggs.add(dTerms);
            } else if (agg instanceof DoubleRareTerms) {
                newAggs.add(agg);
            } else {
                throw new IllegalStateException("Encountered a non-RareTerms numeric agg when reducing RareTerms.");
            }
        }
        return newAggs.get(0).doReduce(newAggs, reduceContext);
    }

    @Override
    public boolean containsTerm(BloomFilter bloom, DoubleTerms.Bucket bucket) {
        return bloom.mightContain(NumericUtils.doubleToSortableLong((double) bucket.getKey()));
    }
}
