/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

public class BucketsAggregatorTests extends AggregatorTestCase{

    public BucketsAggregator buildMergeAggregator() throws IOException{
        try(Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("numeric", 0));
                indexWriter.addDocument(document);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                SearchContext searchContext = createSearchContext(
                    indexSearcher,
                    createIndexSettings(),
                    null,
                    new MultiBucketConsumerService.MultiBucketConsumer(
                        DEFAULT_MAX_BUCKETS,
                        new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                    ),
                    new NumberFieldMapper.NumberFieldType("test", NumberFieldMapper.NumberType.INTEGER)
                );

                return new BucketsAggregator("test", AggregatorFactories.EMPTY, searchContext, null, null, null) {
                    @Override
                    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
                        return null;
                    }

                    @Override
                    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
                        return new InternalAggregation[0];
                    }

                    @Override
                    public InternalAggregation buildEmptyAggregation() {
                        return null;
                    }
                };
            }
        }
    }

    public void testBucketMergeNoDelete() throws IOException{
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        for(int i = 0; i < 10; i++){
            mergeAggregator.incrementBucketDocCount(i, i);
        }

        mergeAggregator.mergeBuckets(10, bucket -> bucket % 5);

        for(int i=0; i<5; i++) {
            // The i'th bucket should now have all docs whose index % 5 = i
            // This is buckets i and i + 5
            // i + (i+5) = 2*i + 5
            assertEquals(mergeAggregator.getDocCounts().get(i), (2 * i) + 5);
        }
        for(int i=5; i<10; i++){
            assertEquals(mergeAggregator.getDocCounts().get(i),  0);
        }
    }

    public void testBucketMergeAndDelete() throws IOException{
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        int sum = 0;
        for(int i = 0; i < 20; i++){
            mergeAggregator.incrementBucketDocCount(i, i);
            if(5 <= i && i < 15) {
                sum += i;
            }
        }

        // Put the buckets in indices 5 ... 14 into bucket 5, and delete the rest of the buckets
        mergeAggregator.mergeBuckets(10, bucket -> (5 <= bucket && bucket < 15) ? 5 : -1);

        assertEquals(mergeAggregator.getDocCounts().size(), 10); // Confirm that the 10 other buckets were deleted
        for(int i=0; i<10; i++){
            assertEquals(mergeAggregator.getDocCounts().get(i), i == 5 ? sum : 0);
        }
    }
}
