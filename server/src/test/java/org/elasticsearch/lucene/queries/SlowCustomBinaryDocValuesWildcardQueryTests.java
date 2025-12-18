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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SlowCustomBinaryDocValuesWildcardQueryTests extends ESTestCase {

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
                        long count = searcher.count(new SlowCustomBinaryDocValuesWildcardQuery(fieldName, entry.getKey() + "*", false));
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
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "a*", false);
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
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "a*", false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testAgainstWildcardQuery() throws IOException {
        List<String> randomValues = randomList(8, 32, () -> randomAlphaOfLength(8));

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (String randomValue : randomValues) {
                    Document document = new Document();
                    document.add(new SortedDocValuesField("baseline_field", new BytesRef(randomValue)));
                    document.add(
                        new BinaryFieldMapper.CustomBinaryDocValuesField("contender_field", randomValue.getBytes(StandardCharsets.UTF_8))
                    );
                    writer.addDocument(document);
                }

                try (IndexReader reader = writer.getReader()) {
                    String randomWildcard = randomFrom(randomValues).substring(0, 2) + "*";
                    IndexSearcher searcher = newSearcher(reader);

                    Query baselineQuery = new WildcardQuery(
                        new Term("baseline_field", randomWildcard),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                    TopDocs baselineResults = searcher.search(baselineQuery, 32);

                    Query contenderQuery = new SlowCustomBinaryDocValuesWildcardQuery("contender_field", randomWildcard, false);
                    TopDocs contenderResults = searcher.search(contenderQuery, 32);

                    assertThat(contenderResults.totalHits, equalTo(baselineResults.totalHits));
                    assertThat(baselineResults.scoreDocs.length, greaterThanOrEqualTo(1));
                    assertThat(baselineResults.scoreDocs.length, equalTo(contenderResults.scoreDocs.length));
                    for (int i = 0; i < baselineResults.scoreDocs.length; i++) {
                        assertThat(baselineResults.scoreDocs[i].doc, equalTo(contenderResults.scoreDocs[i].doc));
                        assertThat(baselineResults.scoreDocs[i].score, equalTo(contenderResults.scoreDocs[i].score));
                    }
                }
            }
        }
    }

}
