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

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link BucketsAggregator} which resolves to the matching parent documents.
 *
 * It ensures that each parent only matches once per bucket.
 */
public class ChildrenToParentAggregator extends AbstractParentChildAggregator {

    public ChildrenToParentAggregator(String name, AggregatorFactories factories,
            SearchContext context, Aggregator parent, Query childFilter,
            Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
            long maxOrd, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, factories, context, parent, childFilter, parentFilter,
            valuesSource, maxOrd, pipelineAggregators, metaData);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalParent(name, bucketDocCount(owningBucketOrdinal),
                bucketAggregations(owningBucketOrdinal), pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalParent(name, 0, buildEmptySubAggregations(), pipelineAggregators(),
                metaData());
    }

    @Override
    LeafBucketCollector getLeafBucketCollector(SortedSetDocValues globalOrdinals, Bits childDocs) {
        return new LeafBucketCollector() {

            @Override
            public void collect(int docId, long bucket) throws IOException {
                if (childDocs.get(docId) && globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    if (globalOrdinal != -1) {
                        long bucketPrev = ordinalToBuckets.get(globalOrdinal);

                        // add bucket if none is set
                        if (bucketPrev == -1) {
                            ordinalToBuckets.set(globalOrdinal, bucket);
                        } else if (bucketPrev != bucket) {
                            // otherwise store it in the array, but only if the bucket is different than
                            // the previous one for this childDoc, as we only need one occurrence
                            // of child -> parent for this aggregation
                            storeToOtherBuckets(globalOrdinal, bucket);
                        }
                    }
                }
            }
        };
    }

    @Override
    Weight getCollectionFilter() {
        return childFilter;
    }

    @Override
    Weight getPostCollectionFilter() {
        return parentFilter;
    }
}
