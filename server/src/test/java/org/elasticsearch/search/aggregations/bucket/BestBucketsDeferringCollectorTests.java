/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;

public class BestBucketsDeferringCollectorTests extends AggregatorTestCase {

    public void testReplay() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numDocs = randomIntBetween(1, 128);
        int maxNumValues = randomInt(16);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", String.valueOf(randomInt(maxNumValues)), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        TermQuery termQuery = new TermQuery(new Term("field", String.valueOf(randomInt(maxNumValues))));
        Query rewrittenQuery = indexSearcher.rewrite(termQuery);
        TopDocs topDocs = indexSearcher.search(termQuery, numDocs);

        BestBucketsDeferringCollector collector = new BestBucketsDeferringCollector(rewrittenQuery, indexSearcher, false) {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
            }
        };
        Set<Integer> deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(termQuery, collector.asCollector());
        collector.postCollection();
        collector.prepareSelectedBuckets(0);

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }

        topDocs = indexSearcher.search(new MatchAllDocsQuery(), numDocs);
        collector = new BestBucketsDeferringCollector(rewrittenQuery, indexSearcher, true);
        deferredCollectedDocIds = new HashSet<>();
        collector.setDeferredCollector(Collections.singleton(bla(deferredCollectedDocIds)));
        collector.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), collector.asCollector());
        collector.postCollection();
        collector.prepareSelectedBuckets(0);

        assertEquals(topDocs.scoreDocs.length, deferredCollectedDocIds.size());
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            assertTrue("expected docid [" + scoreDoc.doc + "] is missing", deferredCollectedDocIds.contains(scoreDoc.doc));
        }
        indexReader.close();
        directory.close();
    }

    private BucketCollector bla(Set<Integer> docIds) {
        return new BucketCollector() {
            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                return new LeafBucketCollector() {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        docIds.add(aggCtx.getLeafReaderContext().docBase + doc);
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
    }

    public void testBucketMergeNoDelete() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.rewriteBuckets(oldBucket -> 0);
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
                    deferringCollector.rewriteBuckets(oldBucket -> oldBucket > 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(0, 8, 9);

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(4, 5, 6, 7), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/60021")
    public void testBucketMergeAndDeleteLastEntry() throws Exception {
        testCase((deferringCollector, delegate) -> new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0; // Only collects at top level
                delegate.collect(doc, doc);
                if (doc == 7) {
                    deferringCollector.rewriteBuckets(oldBucket -> oldBucket <= 3 ? 0 : -1);
                }
            }
        }, (deferringCollector, finalCollector) -> {
            deferringCollector.prepareSelectedBuckets(0, 8, 9);

            assertThat(finalCollector.collection, equalTo(Map.of(0L, List.of(0, 1, 2, 3), 1L, List.of(8), 2L, List.of(9))));
        });
    }

    private void testCase(
        BiFunction<BestBucketsDeferringCollector, LeafBucketCollector, LeafBucketCollector> leafCollector,
        CheckedBiConsumer<BestBucketsDeferringCollector, CollectingBucketCollector, IOException> verify
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
                BestBucketsDeferringCollector deferringCollector = new BestBucketsDeferringCollector(query, indexSearcher, false);

                CollectingBucketCollector finalCollector = new CollectingBucketCollector();
                deferringCollector.setDeferredCollector(Collections.singleton(finalCollector));
                deferringCollector.preCollection();
                indexSearcher.search(query, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        LeafBucketCollector delegate = deferringCollector.getLeafCollector(
                            new AggregationExecutionContext(context, null, null, null)
                        );
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
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
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
        public void postCollection() throws IOException {

        }
    }
}
