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

public class SlowCustomBinaryDocValuesTermInSetQueryTests extends ESTestCase {

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

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
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
                        long count = searcher.count(new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms));
                        assertEquals(entry.getValue().longValue(), count);
                    }
                }

                // search with set containing multiple terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // Query for "a" or "b" should return count of a + count of b
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("b"));
                    long count = searcher.count(new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms));
                    assertEquals(expectedCounts.get("a") + expectedCounts.get("b"), count);

                    // Query for "c", "d", "e" should return sum of their counts
                    List<BytesRef> terms2 = List.of(new BytesRef("c"), new BytesRef("d"), new BytesRef("e"));
                    long count2 = searcher.count(new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms2));
                    assertEquals(expectedCounts.get("c") + expectedCounts.get("d") + expectedCounts.get("e"), count2);
                }

                // search with list containing non-existent term
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("nonexistent"));
                    long count = searcher.count(new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms));
                    assertEquals(0, count);
                }

                // search with list containing mix of existent and non-existent terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("nonexistent"));
                    long count = searcher.count(new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms));
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
                    Query query = new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, List.of(new BytesRef("a")));
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = new NumericDocValuesField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, List.of(new BytesRef("a")));
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testNullTermsThrows() {
        expectThrows(NullPointerException.class, () -> new SlowCustomBinaryDocValuesTermInSetQuery("field", null));
    }

    public void testEqualsAndHashCode() {
        String fieldName = "field";
        List<BytesRef> terms1 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms2 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms3 = List.of(new BytesRef("a"), new BytesRef("c"));

        SlowCustomBinaryDocValuesTermInSetQuery query1 = new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms1);
        SlowCustomBinaryDocValuesTermInSetQuery query2 = new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms2);
        SlowCustomBinaryDocValuesTermInSetQuery query3 = new SlowCustomBinaryDocValuesTermInSetQuery(fieldName, terms3);
        SlowCustomBinaryDocValuesTermInSetQuery query4 = new SlowCustomBinaryDocValuesTermInSetQuery("other", terms1);

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

        SlowCustomBinaryDocValuesTermInSetQuery query1 = new SlowCustomBinaryDocValuesTermInSetQuery("field", termsWithDuplicates);
        SlowCustomBinaryDocValuesTermInSetQuery query2 = new SlowCustomBinaryDocValuesTermInSetQuery("field", termsWithoutDuplicates);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

}
