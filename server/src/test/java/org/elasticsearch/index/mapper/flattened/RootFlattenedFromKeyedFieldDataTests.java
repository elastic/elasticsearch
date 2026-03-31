/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RootFlattenedFromKeyedFieldDataTests extends ESTestCase {

    private static final String KEYED_FIELD = "field._keyed";
    private static final String ROOT_FIELD = "field";

    public void testSingleKeyedValue() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "key1\0value1");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("value1"), values.nextValue());
                }
            }
        }
    }

    public void testMultipleKeysUniqueValues() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "key1\0alpha", "key2\0beta", "key3\0gamma");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertTrue(values.advanceExact(0));
                    assertEquals(3, values.docValueCount());
                    assertEquals(new BytesRef("alpha"), values.nextValue());
                    assertEquals(new BytesRef("beta"), values.nextValue());
                    assertEquals(new BytesRef("gamma"), values.nextValue());
                }
            }
        }
    }

    public void testDuplicateValuesAcrossKeys() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "key1\0foo", "key2\0foo", "key3\0bar");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertTrue(values.advanceExact(0));
                    assertEquals(2, values.docValueCount());
                    assertEquals(new BytesRef("bar"), values.nextValue());
                    assertEquals(new BytesRef("foo"), values.nextValue());
                }
            }
        }
    }

    public void testAllDuplicateValues() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "a\0same", "b\0same", "c\0same");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertTrue(values.advanceExact(0));
                    assertEquals(1, values.docValueCount());
                    assertEquals(new BytesRef("same"), values.nextValue());
                }
            }
        }
    }

    public void testValuesAreSorted() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "key1\0zebra", "key2\0apple", "key3\0mango");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertTrue(values.advanceExact(0));
                    assertEquals(3, values.docValueCount());
                    assertEquals(new BytesRef("apple"), values.nextValue());
                    assertEquals(new BytesRef("mango"), values.nextValue());
                    assertEquals(new BytesRef("zebra"), values.nextValue());
                }
            }
        }
    }

    public void testEmptyDocument() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                writer.addDocument(new Document());

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);
                    assertFalse(values.advanceExact(0));
                }
            }
        }
    }

    public void testMultipleDocuments() throws IOException {
        boolean binary = randomBoolean();
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                addDoc(writer, binary, "a\0x", "b\0y");
                addDoc(writer, binary, "c\0y", "d\0z");

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);

                    assertTrue(values.advanceExact(0));
                    assertEquals(2, values.docValueCount());
                    List<String> doc0 = List.of(values.nextValue().utf8ToString(), values.nextValue().utf8ToString());

                    assertTrue(values.advanceExact(1));
                    assertEquals(2, values.docValueCount());
                    List<String> doc1 = List.of(values.nextValue().utf8ToString(), values.nextValue().utf8ToString());

                    assertEquals(Set.of(List.of("x", "y"), List.of("y", "z")), Set.of(doc0, doc1));
                }
            }
        }
    }

    public void testRandomized() throws IOException {
        boolean binary = randomBoolean();
        int docCount = randomIntBetween(1, 20);

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
                Set<List<String>> expectedValuesPerDoc = new HashSet<>();
                for (int d = 0; d < docCount; d++) {
                    int fieldCount = randomIntBetween(1, 10);
                    String[] keyedValues = new String[fieldCount];
                    Set<String> uniqueValues = new HashSet<>();
                    for (int f = 0; f < fieldCount; f++) {
                        String key = "key" + randomIntBetween(0, 100);
                        String value = randomAlphaOfLengthBetween(1, 20);
                        keyedValues[f] = key + "\0" + value;
                        uniqueValues.add(value);
                    }
                    addDoc(writer, binary, keyedValues);

                    // values should be sorted lexicographically
                    expectedValuesPerDoc.add(uniqueValues.stream().sorted().toList());
                }

                try (IndexReader reader = openReader(writer)) {
                    SortedBinaryDocValues values = loadRootValues(reader, binary);

                    // Merge policies may reorder documents, so collect values
                    // as a set of sorted lists without assuming document order.
                    Set<List<String>> actualValuesPerDoc = new HashSet<>();
                    for (int d = 0; d < docCount; d++) {
                        assertTrue("doc " + d + " should have values", values.advanceExact(d));
                        List<String> actual = new ArrayList<>();
                        for (int i = 0; i < values.docValueCount(); i++) {
                            actual.add(values.nextValue().utf8ToString());
                        }
                        actualValuesPerDoc.add(actual);
                    }

                    assertEquals(expectedValuesPerDoc, actualValuesPerDoc);
                }
            }
        }
    }

    private static RandomIndexWriter newRandomIndexWriter(Directory dir, boolean binary) throws IOException {
        if (binary) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            return new RandomIndexWriter(random(), dir, iwc);
        }
        return new RandomIndexWriter(random(), dir);
    }

    private static void addDoc(RandomIndexWriter writer, boolean binary, String... keyedValues) throws IOException {
        Document doc = new Document();
        if (binary) {
            var field = new MultiValuedBinaryDocValuesField.SeparateCount(KEYED_FIELD, false);
            for (String kv : keyedValues) {
                field.add(new BytesRef(kv));
            }
            doc.add(field);
            doc.add(NumericDocValuesField.indexedField(KEYED_FIELD + ".counts", field.count()));
        } else {
            for (String kv : keyedValues) {
                doc.add(new SortedSetDocValuesField(KEYED_FIELD, new BytesRef(kv)));
            }
        }
        writer.addDocument(doc);
    }

    private static IndexReader openReader(RandomIndexWriter writer) throws IOException {
        writer.forceMerge(1);
        return ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));
    }

    private static SortedBinaryDocValues loadRootValues(IndexReader reader, boolean binary) {
        var builder = new RootFlattenedFromKeyedFieldData.Builder(ROOT_FIELD, KEYED_FIELD, binary);
        IndexFieldData<?> fieldData = builder.build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        return fieldData.load(reader.leaves().get(0)).getBytesValues();
    }
}
