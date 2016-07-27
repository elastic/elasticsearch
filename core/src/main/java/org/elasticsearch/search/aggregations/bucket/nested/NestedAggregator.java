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

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NestedAggregator extends SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final BitSetProducer parentFilter;
    private final Query childFilter;

    public NestedAggregator(String name, AggregatorFactories factories, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper,
                            AggregationContext aggregationContext, Aggregator parentAggregator,
                            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parentAggregator, pipelineAggregators, metaData);
        Query parentFilter = parentObjectMapper != null ? parentObjectMapper.nestedTypeFilter() : Queries.newNonNestedFilter();
        this.parentFilter = context.searchContext().bitsetFilterCache().getBitSetProducer(parentFilter);
        this.childFilter = childObjectMapper.nestedTypeFilter();
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(ctx);
        IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        Weight weight = searcher.createNormalizedWeight(childFilter, false);
        Scorer childDocsScorer = weight.scorer(ctx);

        final BitSet parentDocs = parentFilter.getBitSet(ctx);
        final DocIdSetIterator childDocs = childDocsScorer != null ? childDocsScorer.iterator() : null;
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int parentDoc, long bucket) throws IOException {
                // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent
                // doc), so we can skip:
                if (parentDoc == 0 || parentDocs == null || childDocs == null) {
                    return;
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
        return new InternalNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal),
                pipelineAggregators(), metaData());
    }

        @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

}
