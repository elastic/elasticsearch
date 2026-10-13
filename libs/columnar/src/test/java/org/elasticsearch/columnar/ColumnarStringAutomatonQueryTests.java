/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;

/**
 * A wildcard automaton answered by the column, over the shapes that answer it differently. Every pattern is
 * checked against what Lucene says the pattern means. Narrowing a pattern to a cheaper query is the caller's
 * business and is tested where it is done.
 */
public class ColumnarStringAutomatonQueryTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    /**
     * The patterns worth telling apart: three that name a shape a column answers without an automaton, and
     * four that do not and so have to be run as one.
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

    /**
     * An automaton has no equality to cache on, so the query keys on what produced it. Two queries built from
     * the same pattern have to be the same query, and two built from different patterns have to differ, or a
     * cached filter would be handed to the wrong one.
     */
    public void testCacheIdentityFollowsThePattern() {
        assertEquals(automatonFor("al*a"), automatonFor("al*a"));
        assertEquals(automatonFor("al*a").hashCode(), automatonFor("al*a").hashCode());
        assertNotEquals(automatonFor("al*a"), automatonFor("al*b"));
        assertNotEquals(automatonFor("al*a"), automatonFor("other", "al*a"));
    }

    /**
     * An updated field, read as an overlay of its layers rather than as the column. The automaton has no
     * column to run over then, so it runs over the values a document at a time, and has to find the same
     * documents either way.
     */
    public void testMatchesThroughAnOverlaidColumn() throws IOException {
        final List<String> values = values(between(400, 1200), d -> d % 5 == 0 ? "alpine-" + d : TERMS[d % TERMS.length]);
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, stringPayload(value), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(ColumnarTestUtils.hideTheColumn(reader));
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton through an overlay",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern))
                    );
                }
            }
        }
    }

    /**
     * A single-valued field read as an overlay, whose blobs are each document's value rather than a payload, so the
     * automaton runs over the blob itself. Documents without the field and the empty string are among them.
     */
    public void testMatchesThroughAnOverlaidSingleValuedColumn() throws IOException {
        final List<String> values = values(
            between(400, 1200),
            d -> d % 7 == 3 ? null : d % 11 == 0 ? "" : d % 5 == 0 ? "alpine-" + d : TERMS[d % TERMS.length]
        );
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = ColumnarTestUtils.singleValuedBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(ColumnarTestUtils.hideTheColumn(reader));
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton through an overlay",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern))
                    );
                }
            }
        }
    }

    /** Every pattern, against the documents running Lucene's automaton for it over the values would find. */
    private void assertPatterns(List<String> values) throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(new Field(FIELD, stringPayload(value), type));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern))
                    );
                }
            }
        }
    }

    private static Query automatonFor(String pattern) {
        return automatonFor(FIELD, pattern);
    }

    private static Query automatonFor(String field, String pattern) {
        return new ColumnarStringAutomatonQuery(
            field,
            Operations.determinize(
                WildcardQuery.toAutomaton(new Term(field, pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            ),
            "pattern=" + pattern,
            ScanBudget.UNLIMITED
        );
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
        final TopDocs hits = searcher.search(query, Integer.MAX_VALUE);
        final List<Integer> docs = new ArrayList<>();
        for (ScoreDoc hit : hits.scoreDocs) {
            docs.add(hit.doc);
        }
        docs.sort(Integer::compareTo);
        return docs;
    }
}
