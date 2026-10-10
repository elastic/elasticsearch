/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarStringAutomatonQuery;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.columnar.ScanBudget;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.mapper.SingleValuedColumnarBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.instanceOf;

/**
 * A wildcard pattern as the columnar format answers it. Narrowing a pattern to a term, a prefix or a run of bytes is
 * only worth having while it finds exactly what running the automaton for the pattern would, so every pattern is
 * checked against what Lucene says it means, over each framing the column holds.
 */
public class ColumnarBinaryDocValuesQueriesTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    /**
     * The patterns worth telling apart: three that name a shape a column answers without an automaton, and
     * the rest, which have to be run as one.
     */
    private static final String[] PATTERNS = {
        "alpha",      // a whole value
        "al*",        // a start
        "*lph*",      // a run of bytes
        "*pha",       // an end, which is neither
        "al*a",       // bytes at both ends
        "alph?",      // a single unknown byte
        "*",          // every value
        "zzz*",       // nothing the column holds
        "" };

    private static Query wildcard(String field, String pattern) {
        return ColumnarBinaryDocValuesQueries.INSTANCE.wildcard(field, pattern, false, NoopCircuitBreaker.INSTANCE);
    }

    /**
     * A pattern that names one of the shapes a column answers directly is built as that query, so the column
     * bisects or searches for bytes rather than running an automaton over every distinct value.
     */
    public void testWildcardNarrowsToTheCheapestQuery() {
        assertEquals(ColumnarStringTermQuery.term(FIELD, new BytesRef("alpha"), ScanBudget.UNLIMITED), wildcard(FIELD, "alpha"));
        assertEquals(ColumnarStringTermQuery.prefix(FIELD, new BytesRef("al"), ScanBudget.UNLIMITED), wildcard(FIELD, "al*"));
        // Every value, which is a prefix of no bytes rather than an automaton.
        assertEquals(ColumnarStringTermQuery.prefix(FIELD, new BytesRef(""), ScanBudget.UNLIMITED), wildcard(FIELD, "*"));
        assertEquals(ColumnarStringTermQuery.contains(FIELD, new BytesRef("lph"), ScanBudget.UNLIMITED), wildcard(FIELD, "*lph*"));
    }

    /** A pattern that names none of those shapes stays an automaton. */
    public void testWildcardKeepsTheAutomatonWhereItHasTo() {
        // The empty pattern too: Lucene reads it as naming no value, not as naming the value of no bytes.
        for (String pattern : new String[] { "*pha", "al*a", "alph?", "**", "a*b*c", "al\\*pha", "*a?c*", "" }) {
            assertThat("pattern [" + pattern + "]", wildcard(FIELD, pattern), instanceOf(ColumnarStringAutomatonQuery.class));
        }
        // Case folding is the automaton's business, so even a plain pattern stays one.
        assertThat(
            ColumnarBinaryDocValuesQueries.INSTANCE.wildcard(FIELD, "alpha", true, NoopCircuitBreaker.INSTANCE),
            instanceOf(ColumnarStringAutomatonQuery.class)
        );
    }

    /**
     * An automaton has no equality to cache on, so the query keys on what produced it. Two queries built from the
     * same pattern have to be the same query, and two built from different patterns have to differ, or a cached
     * filter would be handed to the wrong one.
     */
    public void testCacheIdentityFollowsThePattern() {
        assertEquals(wildcard(FIELD, "al*a"), wildcard(FIELD, "al*a"));
        assertEquals(wildcard(FIELD, "al*a").hashCode(), wildcard(FIELD, "al*a").hashCode());
        assertNotEquals(wildcard(FIELD, "al*a"), wildcard(FIELD, "al*b"));
        assertNotEquals(wildcard(FIELD, "al*a"), wildcard("other", "al*a"));
    }

    /** Few distinct values, so the column carries a dictionary and a pattern is run once a term. */
    public void testLowCardinality() throws IOException {
        assertPatterns(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
    }

    /** Every value distinct, so there is no dictionary and a pattern is run against the values. */
    public void testHighCardinality() throws IOException {
        assertPatterns(values(between(300, 1500), d -> "alpha-" + d));
    }

    /** Values in term order, the shape the narrowed term and prefix queries bisect rather than scan. */
    public void testSorted() throws IOException {
        final List<String> sorted = new ArrayList<>(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
        sorted.sort(String::compareTo);
        assertPatterns(sorted);
    }

    /** Hot values over a long tail, so some values are named by the dictionary and the rest escape it. */
    public void testHotValuesWithTail() throws IOException {
        assertPatterns(values(between(600, 2000), d -> d % 40 == 7 ? "alpine-" + d : TERMS[d % TERMS.length]));
    }

    /** Documents without a value, which match nothing however the pattern is answered. */
    public void testSparse() throws IOException {
        assertPatterns(values(between(300, 1500), d -> d % 3 == 0 ? null : TERMS[d % TERMS.length]));
    }

    /** Values of no bytes, which a pattern accepts or does not like any other value. */
    public void testEmptyValues() throws IOException {
        assertPatterns(values(between(300, 1500), d -> d % 4 == 0 ? "" : TERMS[d % TERMS.length]));
    }

    /** A field this segment holds no value for matches nothing, whatever shape the pattern narrows to. */
    public void testFieldAbsentFromTheSegment() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int d = 0; d < 200; d++) {
                    final Document doc = new Document();
                    doc.add(payloadField("other", "v" + d));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String pattern : PATTERNS) {
                    assertEquals("pattern [" + pattern + "]", List.of(), found(searcher, wildcard(FIELD, pattern)));
                }
            }
        }
    }

    /** Every pattern, in both framings the column holds, against what Lucene's automaton for it finds. */
    private void assertPatterns(List<String> values) throws IOException {
        for (boolean singleValued : new boolean[] { false, true }) {
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (String value : values) {
                        final Document doc = new Document();
                        if (value != null) {
                            doc.add(
                                singleValued
                                    ? new SingleValuedColumnarBinaryDocValuesField(FIELD, new BytesRef(value))
                                    : payloadField(FIELD, value)
                            );
                        }
                        writer.addDocument(doc);
                    }
                    writer.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    for (String pattern : PATTERNS) {
                        assertEquals(
                            "pattern [" + pattern + "] singleValued=" + singleValued,
                            accepted(values, pattern),
                            found(searcher, wildcard(FIELD, pattern))
                        );
                    }
                }
            }
        }
    }

    /** The documents whose value Lucene's automaton for the pattern accepts. */
    private static List<Integer> accepted(List<String> values, String pattern) {
        final ByteRunAutomaton automaton = new ByteRunAutomaton(
            Operations.determinize(
                WildcardQuery.toAutomaton(new Term(FIELD, pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            )
        );
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < values.size(); d++) {
            final String value = values.get(d);
            if (value == null) {
                continue;
            }
            final BytesRef bytes = new BytesRef(value);
            if (automaton.run(bytes.bytes, bytes.offset, bytes.length)) {
                docs.add(d);
            }
        }
        return docs;
    }

    private static List<String> values(int count, IntFunction<String> value) {
        final List<String> values = new ArrayList<>(count);
        for (int d = 0; d < count; d++) {
            values.add(value.apply(d));
        }
        return values;
    }

    private static List<Integer> found(IndexSearcher searcher, Query query) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (ScoreDoc hit : searcher.search(query, Integer.MAX_VALUE).scoreDocs) {
            docs.add(hit.doc);
        }
        docs.sort(Integer::compareTo);
        return docs;
    }

    private static Field payloadField(String field, String value) {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return new Field(field, BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(List.of(new BytesRef(value)))), type);
    }

    /** Every doc-values field through the columnar format, which is what makes the field a column. */
    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
