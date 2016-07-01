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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An aggregator of string values.
 */
public class StringTermsAggregator extends AbstractStringTermsAggregator {

    private final ValuesSource valuesSource;
    protected final BytesRefHash bucketOrds;
    private final IncludeExclude.StringFilter includeExclude;

    public StringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
            Terms.Order order, DocValueFormat format, BucketCountThresholds bucketCountThresholds,
            IncludeExclude.StringFilter includeExclude, AggregationContext aggregationContext,
            Aggregator parent, SubAggCollectionMode collectionMode, boolean showTermDocCountError,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError,
                pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        bucketOrds = new BytesRefHash(1, aggregationContext.bigArrays());
    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                values.setDocument(doc);
                final int valuesCount = values.count();

                // SortedBinaryDocValues don't guarantee uniqueness so we need to take care of dups
                previous.clear();
                for (int i = 0; i < valuesCount; ++i) {
                    final BytesRef bytes = values.valueAt(i);
                    if (includeExclude != null && !includeExclude.accept(bytes)) {
                        continue;
                    }
                    if (previous.get().equals(bytes)) {
                        continue;
                    }
                    long bucketOrdinal = bucketOrds.add(bytes);
                    if (bucketOrdinal < 0) { // already seen
                        bucketOrdinal = - 1 - bucketOrdinal;
                        collectExistingBucket(sub, doc, bucketOrdinal);
                    } else {
                        collectBucket(sub, doc, bucketOrdinal);
                    }
                    previous.copyBytes(bytes);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        if (bucketCountThresholds.getMinDocCount() == 0 && (order != InternalOrder.COUNT_DESC || bucketOrds.size() < bucketCountThresholds.getRequiredSize())) {
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searchContext().searcher().getTopReaderContext().leaves()) {
                final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                // brute force
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    values.setDocument(docId);
                    final int valueCount = values.count();
                    for (int i = 0; i < valueCount; ++i) {
                        final BytesRef term = values.valueAt(i);
                        if (includeExclude == null || includeExclude.accept(term)) {
                            bucketOrds.add(term);
                        }
                    }
                }
            }
        }

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        long otherDocCount = 0;
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        StringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new StringTerms.Bucket(new BytesRef(), 0, null, showTermDocCountError, 0, format);
            }
            bucketOrds.get(i, spare.termBytes);
            spare.docCount = bucketDocCount(i);
            otherDocCount += spare.docCount;
            spare.bucketOrd = i;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = (StringTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        // Get the top buckets
        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        long survivingBucketOrds[] = new long[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final StringTerms.Bucket bucket = (StringTerms.Bucket) ordered.pop();
            survivingBucketOrds[i] = bucket.bucketOrd;
            list[i] = bucket;
            otherDocCount -= bucket.docCount;
        }
        // replay any deferred collections
        runDeferredCollections(survivingBucketOrds);

        // Now build the aggs
        for (int i = 0; i < list.length; i++) {
          final StringTerms.Bucket bucket = (StringTerms.Bucket)list[i];
          bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
          bucket.aggregations = bucketAggregations(bucket.bucketOrd);
          bucket.docCountError = 0;
        }

        return new StringTerms(name, order, format, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(),
                bucketCountThresholds.getMinDocCount(), Arrays.asList(list), showTermDocCountError, 0, otherDocCount, pipelineAggregators(),
                metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}

