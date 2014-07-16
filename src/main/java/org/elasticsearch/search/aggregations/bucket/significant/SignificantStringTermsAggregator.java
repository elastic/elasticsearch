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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * An aggregator of significant string values.
 */
public class SignificantStringTermsAggregator extends StringTermsAggregator {

    protected long numCollectedDocs;
    protected final TermFrequencyProvider termFrequencyProvider;
    private final SignificanceHeuristic significanceHeuristic;

    public SignificantStringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
            long estimatedBucketCount, BucketCountThresholds bucketCountThresholds,
            IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent,
            TermFrequencyProvider termFrequencyProvider, SignificanceHeuristic significanceHeuristic) {

        super(name, factories, valuesSource, estimatedBucketCount, null, bucketCountThresholds, includeExclude, aggregationContext, parent, SubAggCollectionMode.DEPTH_FIRST, false);
        this.termFrequencyProvider = termFrequencyProvider;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        super.collect(doc, owningBucketOrdinal);
        numCollectedDocs++;
    }

    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
        long supersetSize = termFrequencyProvider.prepareBackground(context);
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantStringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null);
            }

            bucketOrds.get(i, spare.termBytes);
            spare.subsetDf = bucketDocCount(i);
            spare.subsetSize = subsetSize;
            spare.supersetDf = termFrequencyProvider.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = supersetSize;
            // During shard-local down-selection we use subset/superset stats
            // that are for this shard only
            // Back at the central reducer these properties will be updated with
            // global stats
            spare.updateScore(significanceHeuristic);

            spare.bucketOrd = i;
            if (spare.subsetDf >= bucketCountThresholds.getShardMinDocCount()) {
                spare = (SignificantStringTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantStringTerms.Bucket bucket = (SignificantStringTerms.Bucket) ordered.pop();
            // the terms are owned by the BytesRefHash, we need to pull a copy since the BytesRef hash data may be recycled at some point
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new SignificantStringTerms(subsetSize, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), significanceHeuristic, Arrays.asList(list));
    }

    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(0, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), significanceHeuristic, Collections.<InternalSignificantTerms.Bucket>emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, termFrequencyProvider);
    }

}

