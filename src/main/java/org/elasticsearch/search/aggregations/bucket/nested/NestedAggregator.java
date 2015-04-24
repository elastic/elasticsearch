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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class NestedAggregator extends SingleBucketAggregator {

    private BitDocIdSetFilter parentFilter;
    private final Filter childFilter;

    private DocIdSetIterator childDocs;
    private BitSet parentDocs;

    public NestedAggregator(String name, AggregatorFactories factories, ObjectMapper objectMapper, AggregationContext aggregationContext, Aggregator parentAggregator, Map<String, Object> metaData, QueryCachingPolicy filterCachingPolicy) throws IOException {
        super(name, factories, aggregationContext, parentAggregator, metaData);
        childFilter = aggregationContext.searchContext().filterCache().cache(objectMapper.nestedTypeFilter(), null, filterCachingPolicy);
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // Reset parentFilter, so we resolve the parentDocs for each new segment being searched
        this.parentFilter = null;
        // In ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
        DocIdSet childDocIdSet = childFilter.getDocIdSet(ctx, null);
        if (DocIdSets.isEmpty(childDocIdSet)) {
            childDocs = null;
        } else {
            childDocs = childDocIdSet.iterator();
        }

        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int parentDoc, long bucket) throws IOException {
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
                    Filter parentFilterNotCached = findClosestNestedPath(parent());
                    if (parentFilterNotCached == null) {
                        parentFilterNotCached = Queries.newNonNestedFilter();
                    }
                    parentFilter = context.searchContext().bitsetFilterCache().getBitDocIdSetFilter(parentFilterNotCached);
                    BitDocIdSet parentSet = parentFilter.getDocIdSet(ctx);
                    if (DocIdSets.isEmpty(parentSet)) {
                        // There are no parentDocs in the segment, so return and set childDocs to null, so we exit early for future invocations.
                        childDocs = null;
                        return;
                    } else {
                        parentDocs = parentSet.bits();
                    }
                }

                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                int childDocId = childDocs.docID();
                if (childDocId <= prevParentDoc) {
                    childDocId = childDocs.advance(prevParentDoc + 1);
                }

                for (; childDocId < parentDoc; childDocId = childDocs.nextDoc()) {
                    collectBucket(sub, childDocId, bucket);
                }
            }
        };
    }
        
    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), metaData());
    }

        @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations(), metaData());
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
        private final QueryCachingPolicy queryCachingPolicy;

        public Factory(String name, String path, QueryCachingPolicy queryCachingPolicy) {
            super(name, InternalNested.TYPE.name());
            this.path = path;
            this.queryCachingPolicy = queryCachingPolicy;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, context, parent);
            }
            MapperService.SmartNameObjectMapper mapper = context.searchContext().smartNameObjectMapper(path);
            if (mapper == null) {
                return new Unmapped(name, context, parent, metaData);
            }
            ObjectMapper objectMapper = mapper.mapper();
            if (objectMapper == null) {
                return new Unmapped(name, context, parent, metaData);
            }
            if (!objectMapper.nested().isNested()) {
                throw new AggregationExecutionException("[nested] nested path [" + path + "] is not nested");
            }
            return new NestedAggregator(name, factories, objectMapper, context, parent, metaData, queryCachingPolicy);
        }

        private final static class Unmapped extends NonCollectingAggregator {

            public Unmapped(String name, AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
                super(name, context, parent, metaData);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalNested(name, 0, buildEmptySubAggregations(), metaData());
            }
        }
    }

}
