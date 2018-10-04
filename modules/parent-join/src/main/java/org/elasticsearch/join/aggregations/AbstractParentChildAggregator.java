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

package org.elasticsearch.join.aggregations;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class AbstractParentChildAggregator  extends BucketsAggregator implements SingleBucketAggregator {
    static final ParseField TYPE_FIELD = new ParseField("type");

    protected final Weight childFilter;
    protected final Weight parentFilter;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    // Maybe use PagedGrowableWriter? This will be less wasteful than LongArray,
    // but then we don't have the reuse feature of BigArrays.
    // Also if we know the highest possible value that a parent agg will create
    // then we store multiple values into one slot
    protected final LongArray ordinalToBuckets;

    // Only pay the extra storage price if the a parentOrd has multiple buckets
    // Most of the times a parent doesn't have multiple buckets, since there is
    // only one document per parent ord,
    // only in the case of terms agg if a parent doc has multiple terms per
    // field this is needed:
    protected final LongObjectPagedHashMap<long[]> ordinalToOtherBuckets;
    protected boolean multipleBucketsPerOrdinal = false;

    public AbstractParentChildAggregator(String name, AggregatorFactories factories,
                                         SearchContext context, Aggregator parent, Query childFilter,
                                         Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
                                         long maxOrd, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
        throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        // these two filters are cached in the parser
        this.childFilter = context.searcher().createWeight(context.searcher().rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.parentFilter = context.searcher().createWeight(context.searcher().rewrite(parentFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.ordinalToBuckets = context.bigArrays().newLongArray(maxOrd, false);
        this.ordinalToBuckets.fill(0, maxOrd, -1);
        this.ordinalToOtherBuckets = new LongObjectPagedHashMap<>(context.bigArrays());
        this.valuesSource = valuesSource;
    }

    protected void storeToOtherBuckets(long globalOrdinal, long bucket) {
        long[] bucketOrds = ordinalToOtherBuckets.get(globalOrdinal);
        if (bucketOrds != null) {
            bucketOrds = Arrays.copyOf(bucketOrds, bucketOrds.length + 1);
            bucketOrds[bucketOrds.length - 1] = bucket;
            ordinalToOtherBuckets.put(globalOrdinal, bucketOrds);
        } else {
            ordinalToOtherBuckets.put(globalOrdinal, new long[] { bucket });
        }
        multipleBucketsPerOrdinal = true;
    }

    abstract Weight getCollectionFilter();

    abstract Weight getPostCollectionFilter();

    abstract LeafBucketCollector getLeafBucketCollector(SortedSetDocValues globalOrdinals, Bits docs);

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
        final Bits docs = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), getCollectionFilter().scorerSupplier(ctx));
        return getLeafBucketCollector(globalOrdinals, docs);
    }

    protected void doPostCollection() throws IOException {
        IndexReader indexReader = context().searcher().getIndexReader();
        for (LeafReaderContext ctx : indexReader.leaves()) {
            Scorer docsScorer = getPostCollectionFilter().scorer(ctx);
            if (docsScorer == null) {
                continue;
            }
            DocIdSetIterator docsIter = docsScorer.iterator();

            final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);

            final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
            // Set the scorer, since we now replay only the parent or child docIds
            sub.setScorer(new Scorable() {
                @Override
                public float score() {
                    return 1f;
                }

                @Override
                public int docID() {
                    return docsIter.docID();
                }
            });

            final Bits liveDocs = ctx.reader().getLiveDocs();
            for (int docId = docsIter
                .nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsIter
                .nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    long bucketOrd = ordinalToBuckets.get(globalOrdinal);
                    if (bucketOrd != -1) {
                        collectBucket(sub, docId, bucketOrd);
                        if (multipleBucketsPerOrdinal) {
                            long[] otherBucketOrds = ordinalToOtherBuckets.get(globalOrdinal);
                            if (otherBucketOrds != null) {
                                for (long otherBucketOrd : otherBucketOrds) {
                                    collectBucket(sub, docId, otherBucketOrd);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(ordinalToBuckets, ordinalToOtherBuckets);
    }
}
