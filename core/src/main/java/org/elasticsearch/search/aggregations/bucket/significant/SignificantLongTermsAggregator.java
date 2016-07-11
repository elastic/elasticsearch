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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SignificantLongTermsAggregator extends LongTermsAggregator {

    public SignificantLongTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
            DocValueFormat format, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext, Aggregator parent,
            SignificanceHeuristic significanceHeuristic, SignificantTermsAggregatorFactory termsAggFactory,
            IncludeExclude.LongFilter includeExclude,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, valuesSource, format, null, bucketCountThresholds, aggregationContext, parent,
                SubAggCollectionMode.DEPTH_FIRST, false, includeExclude, pipelineAggregators, metaData);
        this.significanceHeuristic = significanceHeuristic;
        this.termsAggFactory = termsAggFactory;
    }

    protected long numCollectedDocs;
    private final SignificantTermsAggregatorFactory termsAggFactory;
    private final SignificanceHeuristic significanceHeuristic;

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(super.getLeafCollector(ctx, sub), null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                super.collect(doc, bucket);
                numCollectedDocs++;
            }
        };
    }

    @Override
    public SignificantLongTerms buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        long supersetSize = termsAggFactory.getSupersetNumDocs();
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantLongTerms.Bucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            final int docCount = bucketDocCount(i);
            if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                continue;
            }
            if (spare == null) {
                spare = new SignificantLongTerms.Bucket(0, 0, 0, 0, 0, null, format);
            }
            spare.term = bucketOrds.get(i);
            spare.subsetDf = docCount;
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.term);
            spare.supersetSize = supersetSize;
            // During shard-local down-selection we use subset/superset stats that are for this shard only
            // Back at the central reducer these properties will be updated with global stats
            spare.updateScore(significanceHeuristic);

            spare.bucketOrd = i;
            spare = (SignificantLongTerms.Bucket) ordered.insertWithOverflow(spare);
        }

        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantLongTerms.Bucket bucket = (SignificantLongTerms.Bucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new SignificantLongTerms(subsetSize, supersetSize, name, format, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), significanceHeuristic, Arrays.asList(list), pipelineAggregators(),
                metaData());
    }

    @Override
    public SignificantLongTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantLongTerms(0, supersetSize, name, format, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), significanceHeuristic,
                Collections.<InternalSignificantTerms.Bucket> emptyList(), pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, termsAggFactory);
    }

}
