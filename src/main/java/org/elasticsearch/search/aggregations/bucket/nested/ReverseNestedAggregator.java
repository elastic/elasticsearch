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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.SearchParseException;
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
public class ReverseNestedAggregator extends SingleBucketAggregator {

    private final BitDocIdSetFilter parentFilter;

    public ReverseNestedAggregator(String name, AggregatorFactories factories, ObjectMapper objectMapper, AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
        if (objectMapper == null) {
            parentFilter = context.searchContext().bitsetFilterCache().getBitDocIdSetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = context.searchContext().bitsetFilterCache().getBitDocIdSetFilter(objectMapper.nestedTypeFilter());
        }

    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // In ES if parent is deleted, then also the children are deleted, so the child docs this agg receives
        // must belong to parent docs that is alive. For this reason acceptedDocs can be null here.
        BitDocIdSet docIdSet = parentFilter.getDocIdSet(ctx);
        final BitSet parentDocs;
        if (DocIdSets.isEmpty(docIdSet)) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        } else {
            parentDocs = docIdSet.bits();
        }
        final LongIntOpenHashMap bucketOrdToLastCollectedParentDoc = new LongIntOpenHashMap(32);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int childDoc, long bucket) throws IOException {
                // fast forward to retrieve the parentDoc this childDoc belongs to
                final int parentDoc = parentDocs.nextSetBit(childDoc);
                assert childDoc <= parentDoc && parentDoc != DocIdSetIterator.NO_MORE_DOCS;
                if (bucketOrdToLastCollectedParentDoc.containsKey(bucket)) {
                    int lastCollectedParentDoc = bucketOrdToLastCollectedParentDoc.lget();
                    if (parentDoc > lastCollectedParentDoc) {
                        collectBucket(sub, parentDoc, bucket);
                        bucketOrdToLastCollectedParentDoc.lset(parentDoc);
                    }
                } else {
                    collectBucket(sub, parentDoc, bucket);
                    bucketOrdToLastCollectedParentDoc.put(bucket, parentDoc);
                }
            }
        };
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalReverseNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalReverseNested(name, 0, buildEmptySubAggregations(), metaData());
    }

    Filter getParentFilter() {
        return parentFilter;
    }

    public static class Factory extends AggregatorFactory {

        private final String path;

        public Factory(String name, String path) {
            super(name, InternalReverseNested.TYPE.name());
            this.path = path;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            // Early validation
            NestedAggregator closestNestedAggregator = findClosestNestedAggregator(parent);
            if (closestNestedAggregator == null) {
                throw new SearchParseException(context.searchContext(), "Reverse nested aggregation [" + name + "] can only be used inside a [nested] aggregation");
            }

            final ObjectMapper objectMapper;
            if (path != null) {
                MapperService.SmartNameObjectMapper mapper = context.searchContext().smartNameObjectMapper(path);
                if (mapper == null) {
                    return new Unmapped(name, context, parent, metaData);
                }
                objectMapper = mapper.mapper();
                if (objectMapper == null) {
                    return new Unmapped(name, context, parent, metaData);
                }
                if (!objectMapper.nested().isNested()) {
                    throw new AggregationExecutionException("[reverse_nested] nested path [" + path + "] is not nested");
                }
            } else {
                objectMapper = null;
            }
            return new ReverseNestedAggregator(name, factories, objectMapper, context, parent, metaData);
        }

        private final static class Unmapped extends NonCollectingAggregator {

            public Unmapped(String name, AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
                super(name, context, parent, metaData);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalReverseNested(name, 0, buildEmptySubAggregations(), metaData());
            }
        }
    }
}
