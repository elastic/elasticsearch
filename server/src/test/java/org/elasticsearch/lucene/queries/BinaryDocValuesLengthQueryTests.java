/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BinaryDocValuesLengthQueryTests extends ESTestCase {

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "", null, "ab"); // empty string + null + 2-char value
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);   // all-null, immediately before an empty-string
                                                                                          // doc
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "");              // single empty-string value
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                  // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "xyz");           // single 3-char value
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // length 0 matches the empty string in the multi-value doc and the single empty-string doc, but never the all-null
                    // slot (null is distinct from the empty string) — including the all-null doc that immediately precedes it.
                    assertEquals(2, searcher.count(new BinaryDocValuesLengthQuery(fieldName, 0, true)));
                    // length 3 matches only "xyz".
                    assertEquals(1, searcher.count(new BinaryDocValuesLengthQuery(fieldName, 3, true)));
                }
            }
        }
    }

    public void testSingleValued() throws Exception {
        String fieldName = "field";
        int numDocs = randomIntBetween(1, 100);
        List<BytesRef> values = new ArrayList<>();
        int[] lengthToCount = new int[11];
        for (int i = 0; i < numDocs; i++) {
            var val = new BytesRef(randomAlphaOfLength(between(0, 10)));
            values.add(val);
            lengthToCount[val.length]++;
        }

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var val : values) {
                    Document document = new Document();
                    document.add(new BinaryDocValuesField("field", val));
                    writer.addDocument(document);
                }

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (int len = 0; len <= 10; len++) {
                        long numMatches = searcher.count(new BinaryDocValuesLengthQuery(fieldName, len, false));
                        assertEquals(lengthToCount[len], numMatches);
                    }
                }
            }
        }
    }

    public void testMultiValueFieldWithOneValue() throws Exception {
        String fieldName = "field";
        int numDocs = randomIntBetween(1, 100);
        List<BytesRef> values = new ArrayList<>();
        int[] lengthToCount = new int[11];
        for (int i = 0; i < numDocs; i++) {
            var val = new BytesRef(randomAlphaOfLength(between(0, 10)));
            values.add(val);
            lengthToCount[val.length]++;
        }

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var val : values) {
                    Document document = new Document();
                    var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                        "field",
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                    );
                    field.add(val);
                    var countField = NumericDocValuesField.indexedField("field.counts", 1);
                    document.add(field);
                    document.add(countField);
                    writer.addDocument(document);
                }

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (int len = 0; len <= 10; len++) {
                        long numMatches = searcher.count(new BinaryDocValuesLengthQuery(fieldName, len, false));
                        assertEquals(lengthToCount[len], numMatches);
                    }
                }
            }
        }
    }

    public void testMultiValued() throws Exception {
        String fieldName = "field";
        int numDocs = randomIntBetween(1, 100);
        List<List<BytesRef>> values = new ArrayList<>();
        int[] lengthToCount = new int[11];
        for (int i = 0; i < numDocs; i++) {
            int numValues = randomIntBetween(1, 5);
            var valuesForDoc = new ArrayList<BytesRef>();
            Set<Integer> lengths = new HashSet<>();
            values.add(valuesForDoc);
            for (int j = 0; j < numValues; j++) {
                var val = new BytesRef(randomAlphaOfLength(between(0, 10)));
                valuesForDoc.add(val);
                lengths.add(val.length);
            }

            // only increment any length once for a given doc
            for (var length : lengths) {
                lengthToCount[length]++;
            }
        }

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var valuesForDoc : values) {
                    Document document = new Document();
                    var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                        "field",
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                    );
                    for (var val : valuesForDoc) {
                        field.add(val);
                    }
                    var countField = NumericDocValuesField.indexedField("field.counts", field.count());
                    document.add(field);
                    document.add(countField);
                    writer.addDocument(document);
                }

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (int len = 0; len <= 10; len++) {
                        long numMatches = searcher.count(new BinaryDocValuesLengthQuery(fieldName, len, false));
                        assertEquals(lengthToCount[len], numMatches);
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
                    Query query = new BinaryDocValuesLengthQuery(fieldName, 1, false);
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
                var countField = NumericDocValuesField.indexedField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesLengthQuery(fieldName, 1, false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

}
