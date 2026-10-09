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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.lucene.queries.BinaryDocValuesScanCost;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BinaryDvConfirmedQueryTests extends ESTestCase {

    public void testIsAccountedForAsABinaryDocValuesScanCost() {
        Query query = BinaryDvConfirmedQuery.fromWildcardQuery(Queries.ALL_DOCS_INSTANCE, "field", "*", false, false);

        assertThat(
            "every matches() call opens a decoder over the field's full binary doc values, same as the Scanning* queries",
            query,
            instanceOf(BinaryDocValuesScanCost.class)
        );
        assertEquals("field", ((BinaryDocValuesScanCost) query).field());
    }

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

    /**
     * The fuzzy automaton is already UTF-8 byte-level, so wrapping its {@code automaton} in a fresh {@code ByteRunAutomaton} converts it
     * a second time and mis-matches non-ASCII values. It must run the pre-compiled {@code runAutomaton}.
     */
    public void testFuzzyMatchesNonAsciiValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (String value : new String[] { "héllo", "hello", "hèllo", "world" }) {
                    final Document document = new Document();
                    final BytesRef encoded = MultiValuedBinaryDocValuesField.IntegratedCount.encode(List.of(new BytesRef(value)));
                    document.add(new BinaryDocValuesField("field", encoded));
                    writer.addDocument(document);
                }
                try (DirectoryReader reader = writer.getReader()) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    final FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "héllo"), 1, 0, 50, true);
                    final Query query = BinaryDvConfirmedQuery.fromFuzzyQuery(Queries.ALL_DOCS_INSTANCE, "field", "héllo", fuzzy, false);
                    // "world" is more than one edit away; the other three are within one edit of the search term
                    assertThat(searcher.count(query), equalTo(3));
                }
            }
        }
    }

    private static final String TOO_COMPLEX_WILDCARD = "*a????????????*";

    private static final String TOO_COMPLEX_REGEXP = "[ac]*a[ac]{200,500}";

    public void testTooComplexWildcardPatternThrowsBadRequest() throws IOException {
        for (boolean caseInsensitive : new boolean[] { false, true }) {
            Query query = BinaryDvConfirmedQuery.fromWildcardQuery(
                Queries.ALL_DOCS_INSTANCE,
                "field",
                TOO_COMPLEX_WILDCARD,
                caseInsensitive,
                false
            );
            IllegalArgumentException e = expectTooComplex(query);
            assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
        }
    }

    public void testTooComplexRegexpPatternThrowsBadRequest() throws IOException {
        Query query = BinaryDvConfirmedQuery.fromRegexpQuery(
            Queries.ALL_DOCS_INSTANCE,
            "field",
            TOO_COMPLEX_REGEXP,
            RegExp.ALL,
            0,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            false
        );
        IllegalArgumentException e = expectTooComplex(query);
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
    }

    private IllegalArgumentException expectTooComplex(Query query) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                final Document document = new Document();
                document.add(new BinaryDocValuesField("field", new BytesRef("hello")));
                writer.addDocument(document);
                try (DirectoryReader reader = writer.getReader()) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        () -> query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f)
                    );
                    assertThat(e.getMessage(), equalTo("Pattern was too complex to determinize"));
                    assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
                    return e;
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
