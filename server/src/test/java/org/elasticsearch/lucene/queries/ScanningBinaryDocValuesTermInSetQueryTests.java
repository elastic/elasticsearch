/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanningBinaryDocValuesTermInSetQueryTests extends ESTestCase {

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "alpha", null, "beta"); // multi-value with an inline null slot
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);          // all-null, immediately before a match
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "beta");                  // single value stored raw
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                          // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "gamma", "delta");        // multi-value
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // {beta, zeta}: beta is in the multi-value and single-value docs (the all-null doc preceding the latter must not be
                    // matched), zeta is absent.
                    var betaOrZeta = List.of(new BytesRef("beta"), new BytesRef("zeta"));
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, betaOrZeta, true)));
                    // {alpha, delta}: alpha is in the first doc, delta in the last.
                    var alphaOrDelta = List.of(new BytesRef("alpha"), new BytesRef("delta"));
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, alphaOrDelta, true)));
                }
            }
        }
    }

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("a", 2L);
            expectedCounts.put("b", 5L);
            expectedCounts.put("c", 1L);
            expectedCounts.put("d", 3L);
            expectedCounts.put("e", 10L);
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                            "field",
                            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                        );
                        field.add(new BytesRef(entry.getKey().getBytes(StandardCharsets.UTF_8)));
                        var countField = new NumericDocValuesField("field.counts", 1);

                        if (randomBoolean()) {
                            field.add(new BytesRef("z".getBytes(StandardCharsets.UTF_8)));
                            countField.setLongValue(field.count());
                        }
                        document.add(field);
                        document.add(countField);
                        writer.addDocument(document);
                    }
                }

                // search with set containing single term
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (var entry : expectedCounts.entrySet()) {
                        List<BytesRef> terms = List.of(new BytesRef(entry.getKey()));
                        long count = searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, false));
                        assertEquals(entry.getValue().longValue(), count);
                    }
                }

                // search with set containing multiple terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // Query for "a" or "b" should return count of a + count of b
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("b"));
                    long count = searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, false));
                    assertEquals(expectedCounts.get("a") + expectedCounts.get("b"), count);

                    // Query for "c", "d", "e" should return sum of their counts
                    List<BytesRef> terms2 = List.of(new BytesRef("c"), new BytesRef("d"), new BytesRef("e"));
                    long count2 = searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms2, false));
                    assertEquals(expectedCounts.get("c") + expectedCounts.get("d") + expectedCounts.get("e"), count2);
                }

                // search with list containing non-existent term
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("nonexistent"));
                    long count = searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, false));
                    assertEquals(0, count);
                }

                // search with list containing mix of existent and non-existent terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("nonexistent"));
                    long count = searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, false));
                    assertEquals(expectedCounts.get("a").longValue(), count);
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermInSetQuery(fieldName, List.of(new BytesRef("a")), false);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "field",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = new NumericDocValuesField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermInSetQuery(fieldName, List.of(new BytesRef("a")), false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testNullTermsThrows() {
        expectThrows(NullPointerException.class, () -> new ScanningBinaryDocValuesTermInSetQuery("field", null, false));
    }

    public void testEqualsAndHashCode() {
        String fieldName = "field";
        List<BytesRef> terms1 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms2 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms3 = List.of(new BytesRef("a"), new BytesRef("c"));

        ScanningBinaryDocValuesTermInSetQuery query1 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms1, false);
        ScanningBinaryDocValuesTermInSetQuery query2 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms2, false);
        ScanningBinaryDocValuesTermInSetQuery query3 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms3, false);
        ScanningBinaryDocValuesTermInSetQuery query4 = new ScanningBinaryDocValuesTermInSetQuery("other", terms1, false);

        // Same terms, same field
        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());

        // Different terms
        assertNotEquals(query1, query3);

        // Different field
        assertNotEquals(query1, query4);

        // Self equality
        assertEquals(query1, query1);
    }

    public void testDeduplication() {
        // query with duplicates should equal query without duplicates
        List<BytesRef> termsWithDuplicates = Arrays.asList(
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("a")
        );
        List<BytesRef> termsWithoutDuplicates = List.of(new BytesRef("a"), new BytesRef("b"));

        ScanningBinaryDocValuesTermInSetQuery query1 = new ScanningBinaryDocValuesTermInSetQuery("field", termsWithDuplicates, false);
        ScanningBinaryDocValuesTermInSetQuery query2 = new ScanningBinaryDocValuesTermInSetQuery("field", termsWithoutDuplicates, false);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

}
