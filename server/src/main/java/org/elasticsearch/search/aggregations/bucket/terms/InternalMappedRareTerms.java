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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class InternalMappedRareTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>>
    extends InternalMappedTerms<A,B> {

    final long maxDocCount;
    final BloomFilter bloom;

    InternalMappedRareTerms(String name, BucketOrder order, List<PipelineAggregator> pipelineAggregators,
                            Map<String, Object> metaData, DocValueFormat format,
                            List<B> buckets, long maxDocCount, BloomFilter bloom) {
        // TODO is there a way to determine sum_other_doc_count and doc_count_error_upper_bound equivalents for rare based on bloom?
        super(name, order, 0, 1, pipelineAggregators, metaData, format, 0, false, 0, buckets, 0);
        this.maxDocCount = maxDocCount;
        this.bloom = bloom;
    }

    public long getMaxDocCount() {
        return maxDocCount;
    }

    BloomFilter getBloom() {
        return bloom;
    }

    /**
     * Read from a stream.
     */
    InternalMappedRareTerms(StreamInput in, Bucket.Reader<B> bucketReader) throws IOException {
        super(in, bucketReader);
        maxDocCount = in.readLong();
        bloom = new BloomFilter(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        super.writeTermTypeInfoTo(out);
        out.writeLong(maxDocCount);
        bloom.writeTo(out);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<B>> buckets = new HashMap<>();
        InternalTerms<A, B> referenceTerms = null;
        BloomFilter bloomFilter = null;

        for (InternalAggregation aggregation : aggregations) {
            // Unmapped rare terms don't have a bloom filter so we'll skip all this work
            // and save some type casting headaches later.
            if (aggregation.isMapped() == false) {
                continue;
            }

            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (referenceTerms == null && aggregation.getClass().equals(UnmappedRareTerms.class) == false) {
                referenceTerms = terms;
            }
            if (referenceTerms != null &&
                referenceTerms.getClass().equals(terms.getClass()) == false &&
                terms.getClass().equals(UnmappedRareTerms.class) == false) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException("Merging/Reducing the aggregations failed when computing the aggregation ["
                    + referenceTerms.getName() + "] because the field you gave in the aggregation query existed as two different "
                    + "types in two different indices");
            }
            for (B bucket : terms.getBuckets()) {
                List<B> bucketList = buckets.computeIfAbsent(bucket.getKey(), k -> new ArrayList<>());
                bucketList.add(bucket);
            }

            if (bloomFilter == null) {
                bloomFilter = BloomFilter.EmptyBloomFilter(((InternalMappedRareTerms)aggregation).getBloom().getNumBits(),
                    ((InternalMappedRareTerms)aggregation).getBloom().getNumHashFunctions());
            } else {
                bloomFilter.merge(((InternalMappedRareTerms)aggregation).getBloom());
            }
        }

        final List<B> rare = new ArrayList<>();
        for (List<B> sameTermBuckets : buckets.values()) {
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if ((b.getDocCount() <= maxDocCount && containsTerm(bloomFilter, b) == false)) {
                rare.add(b);
                reduceContext.consumeBucketsAndMaybeBreak(1);
            } else if (b.getDocCount() > maxDocCount) {
                // this term has gone over threshold while merging, so add it to the bloom.
                // Note this may happen during incremental reductions too
                addToBloom(bloomFilter, b);
            }
        }
        CollectionUtil.introSort(rare, order.comparator(null));
        return create(name, rare, 0, 0);
    }

    public abstract boolean containsTerm(BloomFilter bloom, B bucket);

    public abstract void addToBloom(BloomFilter bloom, B bucket);
}
