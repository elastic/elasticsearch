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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 */
public class SignificantLongTermsAggregator extends LongTermsAggregator {

    public SignificantLongTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, @Nullable ValueFormat format,
              long estimatedBucketCount, BucketCountThresholds bucketCountThresholds,
              AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggFactory) {

        super(name, factories, valuesSource, format, estimatedBucketCount, null, bucketCountThresholds, aggregationContext, parent);
        this.termsAggFactory = termsAggFactory;
    }

    protected long numCollectedDocs;
    private final SignificantTermsAggregatorFactory termsAggFactory;

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        super.collect(doc, owningBucketOrdinal);
        numCollectedDocs++;
    }

    @Override
    public SignificantLongTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        long supersetSize = termsAggFactory.prepareBackground(context);
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantLongTerms.Bucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new SignificantLongTerms.Bucket(0, 0, 0, 0, 0, null);
            }
            spare.term = bucketOrds.get(i);
            spare.subsetDf = bucketDocCount(i);
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.term);
            spare.supersetSize = supersetSize;
            assert spare.subsetDf <= spare.supersetDf;
            // During shard-local down-selection we use subset/superset stats that are for this shard only
            // Back at the central reducer these properties will be updated with global stats
            spare.updateScore();

            spare.bucketOrd = i;
            if (spare.subsetDf >= bucketCountThresholds.getShardMinDocCount()) {
                spare = (SignificantLongTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantLongTerms.Bucket bucket = (SignificantLongTerms.Bucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new SignificantLongTerms(subsetSize, supersetSize, name, formatter, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Arrays.asList(list));
    }

    @Override
    public SignificantLongTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantLongTerms(0, supersetSize, name, formatter, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalSignificantTerms.Bucket>emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, termsAggFactory);
    }

}
