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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class MergingBucketsDeferringCollectorTests extends AggregatorTestCase {
    public void testBucketMergeNoDelete() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.mergeBuckets(oldBucket -> 0);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(0, 8, 9);

            equalTo(Map.of(0L, List.of(0, 1, 2, 3, 4, 5, 6, 7), 1L, List.of(8), 2L, List.of(9)));
        });
    }

    public void testBucketMergeAndDelete() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.mergeBuckets(oldBucket -> oldBucket > 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(0, 8, 9);

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(4, 5, 6, 7), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    @AwaitsFix(bugUrl="https://github.com/elastic/elasticsearch/issues/60021")
    public void testBucketMergeAndDeleteLastEntry() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.mergeBuckets(oldBucket -> oldBucket <= 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(0, 8, 9);

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(0, 1, 2, 3), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    private void testCase(
        BiFunction<MergingBucketsDeferringCollector, LeafBucketCollector, LeafBucketCollector> leafCollector,
        CheckedBiConsumer<MergingBucketsDeferringCollector, CollectingBucketCollector, IOException> verify
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    indexWriter.addDocument(new Document());
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                Query query = new MatchAllDocsQuery();
                SearchContext searchContext = createSearchContext(indexSearcher, createIndexSettings(), query, null);
                when(searchContext.query()).thenReturn(query);
                MergingBucketsDeferringCollector deferringCollector = new MergingBucketsDeferringCollector(searchContext, false);

                CollectingBucketCollector finalCollector = new CollectingBucketCollector();
                deferringCollector.setDeferredCollector(Collections.singleton(finalCollector));
                deferringCollector.preCollection();
                indexSearcher.search(query, new BucketCollector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public void preCollection() throws IOException {}

                    @Override
                    public void postCollection() throws IOException {}

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(ctx);
                        return leafCollector.apply(deferringCollector, delegate);
                    }
                });
                deferringCollector.postCollection();
                verify.accept(deferringCollector, finalCollector);
            }
        }
    }

    private class CollectingBucketCollector extends BucketCollector {
        final Map<Long, List<Integer>> collection = new HashMap<>();

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    collection.computeIfAbsent(owningBucketOrd, k -> new ArrayList<>()).add(doc);
                }
            };
        }

        @Override
        public void preCollection() throws IOException {}

        @Override
        public void postCollection() throws IOException {}
    }
}
