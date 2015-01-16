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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.docset.DocIdSets;
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
    private Filter parentFilter;
    private final Filter childFilter;

    private Bits childDocs;
    private FixedBitSet parentDocs;

    private AtomicReaderContext reader;

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

        childFilter = aggregationContext.searchContext().filterCache().cache(objectMapper.nestedTypeFilter());
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        // Reset parentFilter, so we resolve the parentDocs for each new segment being searched
        this.parentFilter = null;
        this.reader = reader;
        try {
            // In ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
            childDocs = DocIdSets.toSafeBits(reader.reader(), childFilter.getDocIdSet(reader, null));
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
            parentFilter = context.searchContext().filterCache().cache(parentFilterNotCached);
            // if the filter cache is disabled, we still need to produce bit sets
            parentFilter = new FixedBitSetCachingWrapperFilter(parentFilter);
            DocIdSet docIdSet = parentFilter.getDocIdSet(reader, null);
            if (DocIdSets.isEmpty(docIdSet)) {
                // There are no parentDocs in the segment, so return and set childDocs to null, so we exit early for future invocations.
                childDocs = null;
                return;
            } else {
                parentDocs = (FixedBitSet) docIdSet;
            }
        }

        int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
        int numChildren = 0;
        for (int childDocId = prevParentDoc + 1; childDocId < parentDoc; childDocId++) {
            if (childDocs.get(childDocId)) {
                ++numChildren;
                collectBucketNoCounts(childDocId, bucketOrd);
            }
        }
        incrementBucketDocCount(numChildren, bucketOrd);
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
}
