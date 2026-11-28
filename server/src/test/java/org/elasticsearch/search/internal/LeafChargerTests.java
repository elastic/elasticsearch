/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NamedMatches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.newContextIndexSearcher;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Searches through a {@link ContextIndexSearcher} with a request breaker for a query whose weight charges through
 * {@link ContextIndexSearcher#getLeafCharger}, and checks that it is charged however the query is wrapped, including by wrappers that
 * create its weight themselves and hide it behind their own, and that each leaf releases what it was charged.
 */
public class LeafChargerTests extends ESTestCase {

    private static final int SEGMENTS = 3;
    private static final int DOCS_PER_SEGMENT = 10;
    private static final String FIELD = "f";
    /** Carried by the first document of every segment only. */
    private static final String RARE = "rare";
    /** Carried by every document. */
    private static final String EVERY = "every";

    public void testChargedHoweverWrapped() throws IOException {
        long bytes = randomLongBetween(1, 1 << 20);
        ChargingQuery charging = new ChargingQuery(bytes);
        // Each of the last three creates its child's weight itself and returns a weight of its own.
        for (Query query : List.of(
            charging,
            new BoostQuery(charging, 2f),
            new ConstantScoreQuery(charging),
            NamedMatches.wrapQuery("name", charging),
            // At the top level it scores through the index side.
            new IndexOrDocValuesQuery(charging, everyDoc()),
            new FunctionScoreQuery(charging, new WeightFactorFunction(2f))
        )) {
            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
            assertThat(search(query, breaker), equalTo(SEGMENTS * DOCS_PER_SEGMENT));
            assertThat("charged on every leaf: " + query, breaker.charges(), equalTo(Collections.nCopies(SEGMENTS, bytes)));
        }
    }

    /** Led by a rare term, {@link IndexOrDocValuesQuery} scores through its doc values side, and the index side is never charged. */
    public void testUnusedIndexSideIsNotCharged() throws IOException {
        Query query = new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, RARE)), BooleanClause.Occur.MUST)
            .add(new IndexOrDocValuesQuery(new ChargingQuery(randomLongBetween(1, 1 << 20)), everyDoc()), BooleanClause.Occur.MUST)
            .build();
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        assertThat(search(query, breaker), equalTo(SEGMENTS));
        assertThat(breaker.charges(), empty());
    }

    public void testChargesNothingWithoutBreaker() throws IOException {
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory)) {
            assertThat(ContextIndexSearcher.getLeafCharger(new IndexSearcher(reader)), sameInstance(ContextIndexSearcher.LeafCharger.NONE));
            try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
                assertThat(ContextIndexSearcher.getLeafCharger(searcher), sameInstance(ContextIndexSearcher.LeafCharger.NONE));
            }
        }
    }

    /**
     * Searches {@link #SEGMENTS} unmerged segments with scores, so that {@link FunctionScoreQuery} wraps its child's weight, profiled or
     * not, and checks that every leaf released what it was charged once it was scored, so none of it was left for
     * {@link ContextIndexSearcher#close}.
     */
    private int search(Query query, TrackingCircuitBreaker breaker) throws IOException {
        try (Directory directory = newDirectory(); DirectoryReader reader = index(directory)) {
            try (ContextIndexSearcher searcher = newContextIndexSearcher(reader)) {
                searcher.setCircuitBreaker(breaker);
                if (randomBoolean()) {
                    searcher.setProfiler(new QueryProfiler());
                }
                long hits = searcher.search(query, new TopScoreDocCollectorManager(1, Integer.MAX_VALUE)).totalHits.value();
                assertThat("each leaf must release its charge once it is scored: " + query, breaker.getUsed(), equalTo(0L));
                return Math.toIntExact(hits);
            }
        }
    }

    /**
     * Stands in for the doc values side of an {@link IndexOrDocValuesQuery}: it matches every document, as the charging query does,
     * but is not a {@link MatchAllDocsQuery}, which {@link IndexOrDocValuesQuery#rewrite} would rewrite the whole query to.
     */
    private static Query everyDoc() {
        return new TermQuery(new Term(EVERY, EVERY));
    }

    private DirectoryReader index(Directory directory) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(SEGMENTS * DOCS_PER_SEGMENT)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                    Document document = new Document();
                    document.add(new StringField(FIELD, doc == 0 ? RARE : "common", Field.Store.NO));
                    document.add(new StringField(EVERY, EVERY, Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.flush();
            }
        }
        return DirectoryReader.open(directory);
    }

    /**
     * Matches every document. Its weight charges {@code bytes} through {@link ContextIndexSearcher#getLeafCharger} each time it
     * builds a scorer, and reports a cost far above any lead's, so that {@link IndexOrDocValuesQuery} leaves it unused when led.
     */
    private static final class ChargingQuery extends Query {
        static final String LABEL = "charging-query";

        private final long bytes;

        ChargingQuery(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            ContextIndexSearcher.LeafCharger charger = ContextIndexSearcher.getLeafCharger(searcher);
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    int maxDoc = context.reader().maxDoc();
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) {
                            charger.charge(context, bytes, LABEL);
                            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(maxDoc));
                        }

                        @Override
                        public long cost() {
                            return Integer.MAX_VALUE;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public String toString(String field) {
            return "ChargingQuery(" + bytes + ")";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && bytes == ((ChargingQuery) obj).bytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), bytes);
        }
    }
}
