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
package org.elasticsearch.search.aggregations.bucket.nested;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 *
 */
public class NestedAggregator extends SingleBucketAggregator implements ReaderContextAware {

    private final String nestedPath;
    private final Aggregator parentAggregator;
    private FixedBitSetFilter parentFilter;
    private final Filter childFilter;

    private DocIdSetIterator childDocs;
    private FixedBitSet parentDocs;
    private AtomicReaderContext reader;

    private FixedBitSet rootDocs;
    private int currentRootDoc = -1;
    private final IntObjectOpenHashMap<IntArrayList> childDocIdBuffers = new IntObjectOpenHashMap<>();

    public NestedAggregator(String name, AggregatorFactories factories, String nestedPath, AggregationContext aggregationContext, Aggregator parentAggregator) {
        super(name, factories, aggregationContext, parentAggregator);
        this.nestedPath = nestedPath;
        this.parentAggregator = parentAggregator;
        MapperService.SmartNameObjectMapper mapper = aggregationContext.searchContext().smartNameObjectMapper(nestedPath);
        if (mapper == null) {
            throw new AggregationExecutionException("[nested] nested path [" + nestedPath + "] not found");
        }
        ObjectMapper objectMapper = mapper.mapper();
        if (objectMapper == null) {
            throw new AggregationExecutionException("[nested] nested path [" + nestedPath + "] not found");
        }
        if (!objectMapper.nested().isNested()) {
            throw new AggregationExecutionException("[nested] nested path [" + nestedPath + "] is not nested");
        }

        // TODO: Revise the cache usage for childFilter
        // Typical usage of the childFilter in this agg is that not all parent docs match and because this agg executes
        // in order we are maybe better off not caching? We can then iterate over the posting list and benefit from skip pointers.
        // Even if caching does make sense it is likely that it shouldn't be forced as is today, but based on heuristics that
        // the filter cache maintains that the childFilter should be cached.

        // By caching the childFilter we're consistent with other features and previous versions.
        childFilter = aggregationContext.searchContext().filterCache().cache(objectMapper.nestedTypeFilter());
        // The childDocs need to be consumed in docId order, this ensures that:
        aggregationContext.ensureScoreDocsInOrder();
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        // Reset parentFilter, so we resolve the parentDocs for each new segment being searched
        this.parentFilter = null;
        this.reader = reader;
        try {
            // In ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
            DocIdSet childDocIdSet = childFilter.getDocIdSet(reader, null);
            if (DocIdSets.isEmpty(childDocIdSet)) {
                childDocs = null;
            } else {
                childDocs = childDocIdSet.iterator();
            }
            rootDocs = context.searchContext().fixedBitSetFilterCache().getFixedBitSetFilter(NonNestedDocsFilter.INSTANCE).getDocIdSet(reader, null);
            // We need to reset the current root doc, otherwise we may emit incorrect child docs if the next segment happen to start with the same root doc id value
            currentRootDoc = -1;
            childDocIdBuffers.clear();
        } catch (IOException ioe) {
            throw new AggregationExecutionException("Failed to aggregate [" + name + "]", ioe);
        }
    }

    @Override
    public void collect(int parentDoc, long bucketOrd) throws IOException {
        // here we translate the parent doc to a list of its nested docs, and then call super.collect for evey one of them so they'll be collected

        // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent doc), so we can skip:
        if (parentDoc == 0 || childDocs == null) {
            return;
        }
        if (parentFilter == null) {
            // The aggs are instantiated in reverse, first the most inner nested aggs and lastly the top level aggs
            // So at the time a nested 'nested' aggs is parsed its closest parent nested aggs hasn't been constructed.
            // So the trick is to set at the last moment just before needed and we can use its child filter as the
            // parent filter.

            // Additional NOTE: Before this logic was performed in the setNextReader(...) method, but the the assumption
            // that aggs instances are constructed in reverse doesn't hold when buckets are constructed lazily during
            // aggs execution
            Filter parentFilterNotCached = findClosestNestedPath(parentAggregator);
            if (parentFilterNotCached == null) {
                parentFilterNotCached = NonNestedDocsFilter.INSTANCE;
            }
            parentFilter = context.searchContext().fixedBitSetFilterCache().getFixedBitSetFilter(parentFilterNotCached);
            parentDocs = parentFilter.getDocIdSet(reader, null);
        }

        int numChildren = 0;
        IntArrayList iterator = getChildren(parentDoc);
        final int[] buffer =  iterator.buffer;
        final int size = iterator.size();
        for (int i = 0; i < size; i++) {
            numChildren++;
            collectBucketNoCounts(buffer[i], bucketOrd);
        }
        incrementBucketDocCount(bucketOrd, numChildren);
    }

    @Override
    protected void doClose() {
        childDocIdBuffers.clear();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations());
    }

    public String getNestedPath() {
        return nestedPath;
    }

    private static Filter findClosestNestedPath(Aggregator parent) {
        for (; parent != null; parent = parent.parent()) {
            if (parent instanceof NestedAggregator) {
                return ((NestedAggregator) parent).childFilter;
            } else if (parent instanceof ReverseNestedAggregator) {
                return ((ReverseNestedAggregator) parent).getParentFilter();
            }
        }
        return null;
    }

    public static class Factory extends AggregatorFactory {

        private final String path;

        public Factory(String name, String path) {
            super(name, InternalNested.TYPE.name());
            this.path = path;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            return new NestedAggregator(name, factories, path, context, parent);
        }
    }

    // The aggs framework can collect buckets for the same parent doc id more than once and because the children docs
    // can only be consumed once we need to buffer the child docs. We only need to buffer child docs in the scope
    // of the current root doc.

    // Examples:
    // 1) nested agg wrapped is by terms agg and multiple buckets per document are emitted
    // 2) Multiple nested fields are defined. A nested agg joins back to another nested agg via the reverse_nested agg.
    //      For each child in the first nested agg the second nested agg gets invoked with the same buckets / docids
    private IntArrayList getChildren(final int parentDocId) throws IOException {
        int rootDocId = rootDocs.nextSetBit(parentDocId);
        if (currentRootDoc == rootDocId) {
            final IntArrayList childDocIdBuffer = childDocIdBuffers.get(parentDocId);
            if (childDocIdBuffer != null) {
                return childDocIdBuffer;
            } else {
                // here we translate the parent doc to a list of its nested docs,
                // and then collect buckets for every one of them so they'll be collected
                final IntArrayList newChildDocIdBuffer = new IntArrayList();
                childDocIdBuffers.put(parentDocId, newChildDocIdBuffer);
                int prevParentDoc = parentDocs.prevSetBit(parentDocId - 1);
                int childDocId;
                if (childDocs.docID() > prevParentDoc) {
                    childDocId = childDocs.docID();
                } else {
                    childDocId = childDocs.advance(prevParentDoc + 1);
                }
                for (; childDocId < parentDocId; childDocId = childDocs.nextDoc()) {
                    newChildDocIdBuffer.add(childDocId);
                }
                return newChildDocIdBuffer;
            }
        } else {
            this.currentRootDoc = rootDocId;
            childDocIdBuffers.clear();
            return getChildren(parentDocId);
        }
    }

}
