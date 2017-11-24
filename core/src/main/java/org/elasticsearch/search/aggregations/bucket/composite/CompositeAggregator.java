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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class CompositeAggregator extends BucketsAggregator {
    private final int size;
    private final CompositeValuesSourceConfig[] sources;
    private final List<String> sourceNames;
    private final boolean canEarlyTerminate;

    private final TreeMap<Integer, Integer> keys;
    private final CompositeValuesComparator array;

    private final List<LeafContext> contexts = new ArrayList<>();
    private LeafContext leaf;
    private RoaringDocIdSet.Builder builder;

    CompositeAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
                            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                            int size, CompositeValuesSourceConfig[] sources, List<String> sourceNames,
                            CompositeKey rawAfterKey) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.size = size;
        this.sources = sources;
        this.sourceNames = sourceNames;
        // we use slot 0 to fill the current document (size+1).
        this.array = new CompositeValuesComparator(context.searcher().getIndexReader(), sources, size+1);
        if (rawAfterKey != null) {
            array.setTop(rawAfterKey.values());
        }
        this.keys = new TreeMap<>(array::compare);
        this.canEarlyTerminate = Arrays.stream(sources)
            .allMatch(CompositeValuesSourceConfig::canEarlyTerminate);
    }

    boolean canEarlyTerminate() {
        return canEarlyTerminate;
    }

    private int[] getReverseMuls() {
        return Arrays.stream(sources).mapToInt(CompositeValuesSourceConfig::reverseMul).toArray();
    }

    @Override
    public InternalAggregation buildAggregation(long zeroBucket) throws IOException {
        assert zeroBucket == 0L;

        // Replay all documents that contain at least one top bucket (collected during the first pass).
        grow(keys.size()+1);
        for (LeafContext context : contexts) {
            DocIdSetIterator docIdSetIterator = context.docIdSet.iterator();
            if (docIdSetIterator == null) {
                continue;
            }
            final CompositeValuesSource.Collector collector =
                array.getLeafCollector(context.ctx, getSecondPassCollector(context.subCollector));
            int docID;
            while ((docID = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                collector.collect(docID);
            }
        }

        int num = Math.min(size, keys.size());
        final InternalComposite.InternalBucket[] buckets = new InternalComposite.InternalBucket[num];
        final int[] reverseMuls = getReverseMuls();
        int pos = 0;
        for (int slot : keys.keySet()) {
            CompositeKey key = array.toCompositeKey(slot);
            InternalAggregations aggs = bucketAggregations(slot);
            int docCount = bucketDocCount(slot);
            buckets[pos++] = new InternalComposite.InternalBucket(sourceNames, key, reverseMuls, docCount, aggs);
        }
        return new InternalComposite(name, size, sourceNames, Arrays.asList(buckets), reverseMuls, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        final int[] reverseMuls = getReverseMuls();
        return new InternalComposite(name, size, sourceNames, Collections.emptyList(), reverseMuls, pipelineAggregators(), metaData());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (leaf != null) {
            leaf.docIdSet = builder.build();
            contexts.add(leaf);
        }
        leaf = new LeafContext(ctx, sub);
        builder = new RoaringDocIdSet.Builder(ctx.reader().maxDoc());
        final CompositeValuesSource.Collector inner = array.getLeafCollector(ctx, getFirstPassCollector());
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long zeroBucket) throws IOException {
                assert zeroBucket == 0L;
                inner.collect(doc);
            }
        };
    }

    @Override
    protected void doPostCollection() throws IOException {
        if (leaf != null) {
            leaf.docIdSet = builder.build();
            contexts.add(leaf);
        }
    }

    /**
     * The first pass selects the top N composite buckets from all matching documents.
     * It also records all doc ids that contain a top N composite bucket in a {@link RoaringDocIdSet} in order to be
     * able to replay the collection filtered on the best buckets only.
     */
    private CompositeValuesSource.Collector getFirstPassCollector() {
        return new CompositeValuesSource.Collector() {
            int lastDoc = -1;

            @Override
            public void collect(int doc) throws IOException {

                // Checks if the candidate key in slot 0 is competitive.
                if (keys.containsKey(0)) {
                    // This key is already in the top N, skip it for now.
                    if (doc != lastDoc) {
                        builder.add(doc);
                        lastDoc = doc;
                    }
                    return;
                }
                if (array.hasTop() && array.compareTop(0) <= 0) {
                    // This key is greater than the top value collected in the previous round.
                    if (canEarlyTerminate) {
                        // The index sort matches the composite sort, we can early terminate this segment.
                        throw new CollectionTerminatedException();
                    }
                    // just skip this key for now
                    return;
                }
                if (keys.size() >= size) {
                    // The tree map is full, check if the candidate key should be kept.
                    if (array.compare(0, keys.lastKey()) > 0) {
                        // The candidate key is not competitive
                        if (canEarlyTerminate) {
                            // The index sort matches the composite sort, we can early terminate this segment.
                            throw new CollectionTerminatedException();
                        }
                        // just skip this key
                        return;
                    }
                }

                // The candidate key is competitive
                final int newSlot;
                if (keys.size() >= size) {
                    // the tree map is full, we replace the last key with this candidate.
                    int slot = keys.pollLastEntry().getKey();
                    // and we recycle the deleted slot
                    newSlot = slot;
                } else {
                    newSlot = keys.size() + 1;
                }
                // move the candidate key to its new slot.
                array.move(0, newSlot);
                keys.put(newSlot, newSlot);
                if (doc != lastDoc) {
                    builder.add(doc);
                    lastDoc = doc;
                }
            }
        };
    }


    /**
     * The second pass delegates the collection to sub-aggregations but only if the collected composite bucket is a top bucket (selected
     *  in the first pass).
     */
    private CompositeValuesSource.Collector getSecondPassCollector(LeafBucketCollector subCollector) throws IOException {
        return doc -> {
            Integer bucket = keys.get(0);
            if (bucket != null) {
                // The candidate key in slot 0 is a top bucket.
                // We can defer the collection of this document/bucket to the sub collector
                collectExistingBucket(subCollector, doc, bucket);
            }
        };
    }

    static class LeafContext {
        final LeafReaderContext ctx;
        final LeafBucketCollector subCollector;
        DocIdSet docIdSet;

        LeafContext(LeafReaderContext ctx, LeafBucketCollector subCollector) {
            this.ctx = ctx;
            this.subCollector = subCollector;
        }
    }
}
