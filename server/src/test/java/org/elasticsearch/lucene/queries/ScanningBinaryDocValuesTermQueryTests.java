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
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ScanningBinaryDocValuesTermQueryTests extends ESTestCase {

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

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (var entry : expectedCounts.entrySet()) {
                        long count = searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef(entry.getKey())));
                        assertEquals(entry.getValue().longValue(), count);
                    }
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
                    Query query = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("a"));
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
                    Query query = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("a"));
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    /**
     * Multi-valued docs use ANY-value (OR) semantics: a doc matches if any one of its values
     * exactly equals the term. When any leaf is multi-valued, {@link SlowCustomBinaryDocValuesTermQuery}
     * stays as the slow predicate query (does not rewrite to {@link BinaryDocValuesTermEqualQuery}).
     */
    public void testMultiValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = BinaryDocValuesTermEqualQueryTests.newRandomIndexWriter(dir)) {
                // doc 0: ["hello", "world"] -> "hello" equals term
                BinaryDocValuesTermEqualQueryTests.addMultiValueDoc(writer, fieldName, "hello", "world");
                // doc 1: ["foo", "bar"] -> neither equals term
                BinaryDocValuesTermEqualQueryTests.addMultiValueDoc(writer, fieldName, "foo", "bar");
                // doc 2: ["world", "hello"] -> "hello" equals term
                BinaryDocValuesTermEqualQueryTests.addMultiValueDoc(writer, fieldName, "world", "hello");
                // doc 3: single-valued "hello" -> equals term
                BinaryDocValuesTermEqualQueryTests.addSingleValueDoc(writer, fieldName, "hello");

                BytesRef term = new BytesRef("hello");
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(3, searcher.count(new SlowCustomBinaryDocValuesTermQuery(fieldName, term)));
                }
            }
        }
    }

    /**
     * When any leaf has multi-valued docs, {@link SlowCustomBinaryDocValuesTermQuery#rewrite} must
     * NOT rewrite to the optimized query — it must stay as the slow path to preserve ANY-value
     * semantics.
     */
    public void testDoesNotRewriteWhenMultiValued() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = BinaryDocValuesTermEqualQueryTests.newRandomIndexWriter(dir)) {
                BinaryDocValuesTermEqualQueryTests.addSingleValueDoc(writer, fieldName, "hello");
                BinaryDocValuesTermEqualQueryTests.addMultiValueDoc(writer, fieldName, "hello", "world");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query rewritten = new SlowCustomBinaryDocValuesTermQuery(fieldName, new BytesRef("hello")).rewrite(searcher);
                    assertThat(
                        "must stay slow when any leaf is multi-valued",
                        rewritten,
                        Matchers.instanceOf(SlowCustomBinaryDocValuesTermQuery.class)
                    );
                }
            }
        }
    }
}
