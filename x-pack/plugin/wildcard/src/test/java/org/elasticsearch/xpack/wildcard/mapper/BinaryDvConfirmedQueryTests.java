/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinaryDvConfirmedQueryTests extends ESTestCase {

    public void testNoBinaryDocValuesOpenedDuringPlanning() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                final Document document = new Document();
                document.add(new BinaryDocValuesField("field", new BytesRef("hello")));
                writer.addDocument(document);
                try (DirectoryReader reader = forbidBinaryDvOpenReader(writer.getReader())) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    final Query query = BinaryDvConfirmedQuery.fromWildcardQuery(Queries.ALL_DOCS_INSTANCE, "field", "*", false, false);
                    final Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    for (LeafReaderContext ctx : reader.leaves()) {
                        weight.scorerSupplier(ctx);
                    }
                }
            }
        }
    }

    // Subsequence pattern creates ~256 DFA states, triggering the CB at state 64.
    private static final String COMPLEX_WILDCARD = "*a*b*c*d*e*f*g*h*";

    public void testCircuitBreakerConsultedForWildcardDuringCreateWeight() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                doc.add(new BinaryDocValuesField("field", new BytesRef("hello")));
                writer.addDocument(doc);
                try (IndexReader reader = writer.getReader()) {
                    AtomicBoolean breakerCalled = new AtomicBoolean();
                    NoopCircuitBreaker trackingBreaker = new NoopCircuitBreaker("test") {
                        @Override
                        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                            breakerCalled.set(true);
                        }
                    };
                    ContextIndexSearcher searcher = new ContextIndexSearcher(
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        true
                    );
                    searcher.setCircuitBreaker(trackingBreaker);
                    Query query = BinaryDvConfirmedQuery.fromWildcardQuery(
                        Queries.ALL_DOCS_INSTANCE,
                        "field",
                        COMPLEX_WILDCARD,
                        false,
                        false
                    );
                    query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    assertTrue(
                        "circuit breaker should be consulted during wildcard automaton construction in createWeight",
                        breakerCalled.get()
                    );
                }
            }
        }
    }

    public void testCircuitBreakerConsultedForRegexpDuringCreateWeight() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                doc.add(new BinaryDocValuesField("field", new BytesRef("hello")));
                writer.addDocument(doc);
                try (IndexReader reader = writer.getReader()) {
                    AtomicBoolean breakerCalled = new AtomicBoolean();
                    NoopCircuitBreaker trackingBreaker = new NoopCircuitBreaker("test") {
                        @Override
                        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                            breakerCalled.set(true);
                        }
                    };
                    ContextIndexSearcher searcher = new ContextIndexSearcher(
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        true
                    );
                    searcher.setCircuitBreaker(trackingBreaker);
                    // a{70} creates 71 DFA states, triggering the CB at state 64.
                    Query query = BinaryDvConfirmedQuery.fromRegexpQuery(
                        Queries.ALL_DOCS_INSTANCE,
                        "field",
                        "a{70}",
                        RegExp.ALL,
                        0,
                        10000,
                        false
                    );
                    query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    assertTrue(
                        "circuit breaker should be consulted during regexp automaton construction in createWeight",
                        breakerCalled.get()
                    );
                }
            }
        }
    }

    private static DirectoryReader forbidBinaryDvOpenReader(DirectoryReader reader) throws IOException {
        return new FilterDirectoryReader(reader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String field) {
                        throw new AssertionError(
                            "getBinaryDocValues() must not be called during scorerSupplier() (planning phase);"
                                + " defer reader construction to ScorerSupplier#get(). field=["
                                + field
                                + "]"
                        );
                    }

                    @Override
                    public IndexReader.CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public IndexReader.CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public IndexReader.CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
