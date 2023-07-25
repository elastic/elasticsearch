/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;

public class SearchTimeoutTests extends IndexShardTestCase {

    private Directory dir;
    private IndexReader reader;
    private IndexShard indexShard;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (reader != null) {
            reader.close();
        }
        dir.close();
        closeShards(indexShard);
    }

    private int indexDocs() throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(500, 2500);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            doc.add(new StringField("field", Integer.toString(i), Field.Store.NO));
            w.addDocument(doc);
        }
        w.close();
        reader = DirectoryReader.open(dir);
        return numDocs;
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            LuceneTestCase.MAYBE_CACHE_POLICY,
            true
        );
    }

    public void testCreateWeightTimeout() throws IOException {
        int numDocs = indexDocs();
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        final boolean[] pulledFirstScorer = new boolean[] { false };
        // We create a custom match_all query whose weight does not override count, and whose scorer allows us to trigger a timeout
        // once the first segment's scorer has been pulled.
        Query query = new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new ConstantScoreWeight(this, boost) {
                    boolean firstSegment = true;

                    @Override
                    public Scorer scorer(LeafReaderContext context) throws IOException {
                        if (firstSegment == false) {
                            pulledFirstScorer[0] = true;
                        }
                        // TODO timeout is so far only caused by pulling terms enum. Should we test other triggers e.g. points ?
                        // How close is this to a real-life scenario? It seems very artificial. Are we really going to have partial results
                        // exposed on timeout triggered by the exitable directory reader or is that only the case when bulk scoring?
                        final TermsEnum termsEnum = context.reader().terms("field").iterator();
                        termsEnum.next();
                        firstSegment = false;
                        return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                    }

                    @Override
                    public boolean isCacheable(LeafReaderContext ctx) {
                        return false;
                    }
                };
            }

            @Override
            public String toString(String field) {
                return "match_all";
            }

            @Override
            public boolean equals(Object o) {
                return sameClassAs(o);
            }

            @Override
            public int hashCode() {
                return classHash();
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.visitLeaf(this);
            }
        };
        {
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader));
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(query));
            context.setSize(size);
            QueryPhase.executeQuery(context);
            assertFalse(context.queryResult().searchTimedOut());
            assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value);
            assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
        }
        {
            pulledFirstScorer[0] = false;
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader)) {
                @Override
                public long getRelativeTimeInMillis() {
                    return pulledFirstScorer[0] ? 1L : 0L;
                }
            };
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(query));
            context.setSize(size);
            QueryPhase.executeQuery(context);
            assertTrue(context.queryResult().searchTimedOut());
            int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
            assertEquals(Math.min(2048, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.totalHits.value);
            assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
        }
    }

    public void testBulkScorerTimeout() throws IOException {
        int numDocs = indexDocs();
        final boolean[] scoredFirstBatch = new boolean[] { false };
        // We create a custom match_all query whose weight does not override count, and whose bulk scorer allows us to trigger a timeout
        // once the first segment, or batch of documents (for segments with more than 2048 docs) have been scored.
        Query query = new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new ConstantScoreWeight(this, boost) {
                    @Override
                    public BulkScorer bulkScorer(LeafReaderContext context) {
                        final float score = score();
                        final int maxDoc = context.reader().maxDoc();
                        return new BulkScorer() {
                            @Override
                            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                                max = Math.min(max, maxDoc);
                                ScoreAndDoc scorer = new ScoreAndDoc();
                                scorer.score = score;
                                collector.setScorer(scorer);
                                for (int doc = min; doc < max; ++doc) {
                                    scorer.doc = doc;
                                    if (acceptDocs == null || acceptDocs.get(doc)) {
                                        collector.collect(doc);
                                    }
                                }
                                scoredFirstBatch[0] = true;
                                return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
                            }

                            @Override
                            public long cost() {
                                return 0;
                            }
                        };
                    }

                    @Override
                    public Scorer scorer(LeafReaderContext context) {
                        return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                    }

                    @Override
                    public boolean isCacheable(LeafReaderContext ctx) {
                        return false;
                    }
                };
            }

            @Override
            public String toString(String field) {
                return "match_all";
            }

            @Override
            public boolean equals(Object o) {
                return sameClassAs(o);
            }

            @Override
            public int hashCode() {
                return classHash();
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.visitLeaf(this);
            }
        };

        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        {
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader));
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(query));
            context.setSize(size);
            QueryPhase.executeQuery(context);
            assertFalse(context.queryResult().searchTimedOut());
            assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value);
            assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
        }
        {
            scoredFirstBatch[0] = false;
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader)) {
                @Override
                public long getRelativeTimeInMillis() {
                    return scoredFirstBatch[0] ? 1L : 0L;
                }
            };
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            context.parsedQuery(new ParsedQuery(query));
            context.setSize(size);
            QueryPhase.executeQuery(context);
            assertTrue(context.queryResult().searchTimedOut());
            int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
            assertEquals(Math.min(2048, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.totalHits.value);
            assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
        }
    }

    private static class ScoreAndDoc extends Scorable {
        float score;
        int doc = -1;

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public float score() {
            return score;
        }
    }
}
