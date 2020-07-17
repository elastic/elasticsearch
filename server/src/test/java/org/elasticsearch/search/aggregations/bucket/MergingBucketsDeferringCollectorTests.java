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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.util.LuceneTestCase.newDirectory;
import static org.mockito.Mockito.when;

public class MergingBucketsDeferringCollectorTests extends AggregatorTestCase {

    public MergingBucketsDeferringCollector getMergingBucketsDeferringCollector(SearchContext searchContext){

        return new MergingBucketsDeferringCollector(searchContext, false) {

            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                super.getLeafCollector(ctx);

                return new LeafBucketCollector() {
                    int lastDoc = 0;

                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        // Force each doc to be collected into a different ordinal so that there are buckets to merge
                        // Otherwise, they will all be collected into ordinal 0 by default
                        bucket = doc;

                        if (context == null) {
                            context = ctx;
                            docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                            bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                        }
                        docDeltasBuilder.add(doc - lastDoc);
                        bucketsBuilder.add(bucket);
                        lastDoc = doc;
                        maxBucket = Math.max(maxBucket, bucket);
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };
    }

    public void testBucketMergeNoDelete() throws Exception {
        try (Directory directory = newDirectory()) {
            int numDocs = 10;
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", 3 * i));
                    indexWriter.addDocument(document);
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                Query query = new MatchAllDocsQuery();
                Query rewrittenQuery = indexSearcher.rewrite(query);

                SearchContext searchContext = createSearchContext(indexSearcher, createIndexSettings(), rewrittenQuery, null);
                when(searchContext.query()).thenReturn(rewrittenQuery);
                MergingBucketsDeferringCollector deferringCollector = getMergingBucketsDeferringCollector(searchContext);

                BucketCollector bc = new BucketCollector() {

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                if (doc == 9) {
                                    deferringCollector.mergeBuckets(b -> 9 - b);
                                }
                            }
                        };
                    }

                    @Override
                    public void preCollection() throws IOException {
                    }

                    @Override
                    public void postCollection() throws IOException {
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                };

                deferringCollector.setDeferredCollector(Collections.singleton(bc));
                deferringCollector.preCollection();
                indexSearcher.search(query, deferringCollector);
                deferringCollector.postCollection();
                deferringCollector.prepareSelectedBuckets(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

                List<Long> ordinals = deferringCollector.getBuckets();
                assertEquals(ordinals.size(), 10);
                for (int i = 0; i < ordinals.size(); i++) {
                    assertEquals(i, 9 - ordinals.get(i));
                }
            }
        }
    }

    public void testBucketMergeAndDelete() throws Exception {
        try (Directory directory = newDirectory()) {
            int numDocs = 10;
            try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document document = new Document();
                    document.add(new NumericDocValuesField("field", 3 * i));
                    indexWriter.addDocument(document);
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                Query query = new MatchAllDocsQuery();
                Query rewrittenQuery = indexSearcher.rewrite(query);

                SearchContext searchContext = createSearchContext(indexSearcher, createIndexSettings(), rewrittenQuery, null);
                when(searchContext.query()).thenReturn(rewrittenQuery);
                MergingBucketsDeferringCollector deferringCollector = getMergingBucketsDeferringCollector(searchContext);

                BucketCollector bc = new BucketCollector() {

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                        return new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                bucket = doc;
                                if (doc == 9) {
                                    // Apply two merge operations once we reach the last bucket
                                    // In the end, only the buckets where (bucket % 4 = 0) will remain
                                    deferringCollector.mergeBuckets(b -> b % 2 == 0 ? b : -1);
                                    deferringCollector.mergeBuckets(b -> b % 4 == 0 ? b : -1);
                                }
                            }
                        };
                    }

                    @Override
                    public void preCollection() throws IOException {
                    }

                    @Override
                    public void postCollection() throws IOException {
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                };

                deferringCollector.setDeferredCollector(Collections.singleton(bc));
                deferringCollector.preCollection();
                indexSearcher.search(query, deferringCollector);
                deferringCollector.postCollection();
                deferringCollector.prepareSelectedBuckets(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

                List<Long> ordinals = deferringCollector.getBuckets();
                assertEquals(ordinals.size(), 3);
                assertEquals(0L, (long)ordinals.get(0));
                assertEquals(4L, (long)ordinals.get(1));
                assertEquals(8L, (long)ordinals.get(2));
            }
        }
    }
}

