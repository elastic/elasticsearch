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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.lucene.queries.BinaryDocValuesContainsTermQuery.contains;

public class BinaryDocValuesContainsTermQueryTests extends ESTestCase {

    public void testContainsExactMatch() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("foobar");
        assertTrue(contains(value, term));
    }

    public void testContainsPrefix() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("foo");
        assertTrue(contains(value, term));
    }

    public void testContainsSuffix() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("bar");
        assertTrue(contains(value, term));
    }

    public void testContainsMiddle() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("oob");
        assertTrue(contains(value, term));
    }

    public void testContainsSingleChar() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("b");
        assertTrue(contains(value, term));
    }

    public void testContainsNoMatch() {
        BytesRef value = new BytesRef("foobar");
        BytesRef term = new BytesRef("baz");
        assertFalse(contains(value, term));
    }

    public void testContainsTermLongerThanValue() {
        BytesRef value = new BytesRef("foo");
        BytesRef term = new BytesRef("foobar");
        assertFalse(contains(value, term));
    }

    public void testContainsEmptyValue() {
        BytesRef value = new BytesRef("");
        BytesRef term = new BytesRef("foo");
        assertFalse(contains(value, term));
    }

    public void testWithOffset() {
        byte[] bytes = "1234567890research".getBytes(StandardCharsets.UTF_8);
        BytesRef value = new BytesRef(bytes.length + 10);
        System.arraycopy(bytes, 0, value.bytes, 10, bytes.length);
        value.length = bytes.length;
        value.offset = 10;
        BytesRef term = new BytesRef("search");
        assertTrue(contains(value, term));
    }

    public void testSingleValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                // doc 0: "elasticsearch" -> contains "search"
                addSingleValueDoc(writer, fieldName, "elasticsearch");
                // doc 1: "kibana" -> does not contain "search"
                addSingleValueDoc(writer, fieldName, "kibana");
                // doc 2: "research" -> contains "search"
                addSingleValueDoc(writer, fieldName, "research");
                // doc 3: "logstash" -> does not contain "search"
                addSingleValueDoc(writer, fieldName, "logstash");
                // doc 4: "searching" -> contains "search"
                addSingleValueDoc(writer, fieldName, "searching");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef("search"));
                    assertEquals(3, searcher.count(query));
                }
            }
        }
    }

    public void testMultiValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                // doc 0: ["hello", "world"] -> "world" does not contain "ell", "hello" does
                addMultiValueDoc(writer, fieldName, "hello", "world");
                // doc 1: ["foo", "bar"] -> neither contains "ell"
                addMultiValueDoc(writer, fieldName, "foo", "bar");
                // doc 2: ["wellcome", "to"] -> "wellcome" contains "ell"
                addMultiValueDoc(writer, fieldName, "wellcome", "to");
                // doc 3: ["abc"] -> does not contain "ell"
                addSingleValueDoc(writer, fieldName, "abc");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef("ell"));
                    assertEquals(2, searcher.count(query));
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef("test"));
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in one segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "testing");
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef("test"));
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testNoMatch() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");
                addMultiValueDoc(writer, fieldName, "foo", "bar");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new BinaryDocValuesContainsTermQuery(fieldName, new BytesRef("xyz"));
                    assertEquals(0, searcher.count(query));
                }
            }
        }
    }

    public void testEqualsAndHashCode() {
        BinaryDocValuesContainsTermQuery q1 = new BinaryDocValuesContainsTermQuery("field", new BytesRef("term"));
        BinaryDocValuesContainsTermQuery q2 = new BinaryDocValuesContainsTermQuery("field", new BytesRef("term"));
        BinaryDocValuesContainsTermQuery q3 = new BinaryDocValuesContainsTermQuery("other", new BytesRef("term"));
        BinaryDocValuesContainsTermQuery q4 = new BinaryDocValuesContainsTermQuery("field", new BytesRef("other"));

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, q3);
        assertNotEquals(q1, q4);
    }

    private static void addSingleValueDoc(RandomIndexWriter writer, String fieldName, String value) throws IOException {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(fieldName, false);
        field.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        var countField = NumericDocValuesField.indexedField(fieldName + ".counts", 1);
        document.add(field);
        document.add(countField);
        writer.addDocument(document);
    }

    private static void addMultiValueDoc(RandomIndexWriter writer, String fieldName, String... values) throws IOException {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(fieldName, false);
        for (String value : values) {
            field.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        }
        var countField = NumericDocValuesField.indexedField(fieldName + ".counts", field.count());
        document.add(field);
        document.add(countField);
        writer.addDocument(document);
    }

    private static RandomIndexWriter newRandomIndexWriter(Directory dir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new RandomIndexWriter(random(), dir, iwc);
    }

}
