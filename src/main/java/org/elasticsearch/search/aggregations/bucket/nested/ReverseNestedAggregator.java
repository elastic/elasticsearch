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

import com.carrotsearch.hppc.LongIntOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 *
 */
public class ReverseNestedAggregator extends SingleBucketAggregator implements ReaderContextAware {

    private final FixedBitSetFilter parentFilter;
    // It is ok to use bitset from bitset cache, because in this agg the path always to a nested parent path.
    private FixedBitSet parentDocs;

    // TODO: Add LongIntPagedHashMap?
    private final Recycler.V<LongIntOpenHashMap> bucketOrdToLastCollectedParentDocRecycler;
    private final LongIntOpenHashMap bucketOrdToLastCollectedParentDoc;

    public ReverseNestedAggregator(String name, AggregatorFactories factories, ObjectMapper objectMapper, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        if (objectMapper == null) {
            parentFilter = context.searchContext().fixedBitSetFilterCache().getFixedBitSetFilter(NonNestedDocsFilter.INSTANCE);
        } else {
            parentFilter = context.searchContext().fixedBitSetFilterCache().getFixedBitSetFilter(objectMapper.nestedTypeFilter());
        }
        bucketOrdToLastCollectedParentDocRecycler = aggregationContext.searchContext().cacheRecycler().longIntMap(32);
        bucketOrdToLastCollectedParentDoc = bucketOrdToLastCollectedParentDocRecycler.v();
        aggregationContext.ensureScoreDocsInOrder();
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        bucketOrdToLastCollectedParentDoc.clear();
        try {
            // In ES if parent is deleted, then also the children are deleted, so the child docs this agg receives
            // must belong to parent docs that is alive. For this reason acceptedDocs can be null here.
            parentDocs = parentFilter.getDocIdSet(reader, null);
        } catch (IOException ioe) {
            throw new AggregationExecutionException("Failed to aggregate [" + name + "]", ioe);
        }
    }

    @Override
    public void collect(int childDoc, long bucketOrd) throws IOException {
        if (parentDocs == null) {
            return;
        }

        // fast forward to retrieve the parentDoc this childDoc belongs to
        final int parentDoc = parentDocs.nextSetBit(childDoc);
        assert childDoc <= parentDoc && parentDoc != DocIdSetIterator.NO_MORE_DOCS;
        if (bucketOrdToLastCollectedParentDoc.containsKey(bucketOrd)) {
            int lastCollectedParentDoc = bucketOrdToLastCollectedParentDoc.lget();
            if (parentDoc > lastCollectedParentDoc) {
                innerCollect(parentDoc, bucketOrd);
                bucketOrdToLastCollectedParentDoc.lset(parentDoc);
            }
        } else {
            innerCollect(parentDoc, bucketOrd);
            bucketOrdToLastCollectedParentDoc.put(bucketOrd, parentDoc);
        }
    }

    private void innerCollect(int parentDoc, long bucketOrd) throws IOException {
        collectBucket(parentDoc, bucketOrd);
    }

    private static NestedAggregator findClosestNestedAggregator(Aggregator parent) {
        for (; parent != null; parent = parent.parent()) {
            if (parent instanceof NestedAggregator) {
                return (NestedAggregator) parent;
            }
        }
        return null;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalReverseNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalReverseNested(name, 0, buildEmptySubAggregations());
    }

    Filter getParentFilter() {
        return parentFilter;
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrdToLastCollectedParentDocRecycler);
    }

    public static class Factory extends AggregatorFactory {

        private final String path;

        public Factory(String name, String path) {
            super(name, InternalReverseNested.TYPE.name());
            this.path = path;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            // Early validation
            NestedAggregator closestNestedAggregator = findClosestNestedAggregator(parent);
            if (closestNestedAggregator == null) {
                throw new SearchParseException(context.searchContext(), "Reverse nested aggregation [" + name + "] can only be used inside a [nested] aggregation");
            }

            final ObjectMapper objectMapper;
            if (path != null) {
                MapperService.SmartNameObjectMapper mapper = context.searchContext().smartNameObjectMapper(path);
                if (mapper == null) {
                    return new Unmapped(name, context, parent);
                }
                objectMapper = mapper.mapper();
                if (objectMapper == null) {
                    return new Unmapped(name, context, parent);
                }
                if (!objectMapper.nested().isNested()) {
                    throw new AggregationExecutionException("[reverse_nested] nested path [" + path + "] is not nested");
                }
            } else {
                objectMapper = null;
            }
            return new ReverseNestedAggregator(name, factories, objectMapper, context, parent);
        }

        private final static class Unmapped extends NonCollectingAggregator {

            public Unmapped(String name, AggregationContext context, Aggregator parent) {
                super(name, context, parent);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalReverseNested(name, 0, buildEmptySubAggregations());
            }
        }
    }
}
