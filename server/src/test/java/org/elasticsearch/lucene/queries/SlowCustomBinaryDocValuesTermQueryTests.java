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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SlowCustomBinaryDocValuesTermQueryTests extends ESTestCase {

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
                        var field = new BinaryFieldMapper.CustomBinaryDocValuesField(
                            "field",
                            entry.getKey().getBytes(StandardCharsets.UTF_8)
                        );
                        if (randomBoolean()) {
                            field.add("z".getBytes(StandardCharsets.UTF_8));
                        }
                        document.add(field);
                        writer.addDocument(document);
                    }
                }

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (var entry : expectedCounts.entrySet()) {
                        long count = searcher.count(new SlowCustomBinaryDocValuesTermQuery(fieldName, new BytesRef(entry.getKey())));
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
                    Query query = new SlowCustomBinaryDocValuesTermQuery(fieldName, new BytesRef("a"));
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document document = new Document();
                document.add(new BinaryFieldMapper.CustomBinaryDocValuesField("field", "a".getBytes(StandardCharsets.UTF_8)));
                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesTermQuery(fieldName, new BytesRef("a"));
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

}
