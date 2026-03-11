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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
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

    public void testSingleKeyedValue() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("key1\0value1");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("value1"), values.nextValue());
        }
    }

    public void testMultipleKeysUniqueValues() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("key1\0alpha", "key2\0beta", "key3\0gamma");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertTrue(values.advanceExact(0));
            assertEquals(3, values.docValueCount());
            assertEquals(new BytesRef("alpha"), values.nextValue());
            assertEquals(new BytesRef("beta"), values.nextValue());
            assertEquals(new BytesRef("gamma"), values.nextValue());
        }
    }

    public void testDuplicateValuesAcrossKeys() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("key1\0foo", "key2\0foo", "key3\0bar");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(new BytesRef("bar"), values.nextValue());
            assertEquals(new BytesRef("foo"), values.nextValue());
        }
    }

    public void testAllDuplicateValues() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("a\0same", "b\0same", "c\0same");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("same"), values.nextValue());
        }
    }

    public void testValuesAreSorted() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("key1\0zebra", "key2\0apple", "key3\0mango");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertTrue(values.advanceExact(0));
            assertEquals(3, values.docValueCount());
            assertEquals(new BytesRef("apple"), values.nextValue());
            assertEquals(new BytesRef("mango"), values.nextValue());
            assertEquals(new BytesRef("zebra"), values.nextValue());
        }
    }

    public void testEmptyDocument() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addEmptyDoc();
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            assertFalse(values.advanceExact(0));
        }
    }

    public void testMultipleDocuments() throws IOException {
        boolean binary = randomBoolean();
        try (TestIndex testIndex = new TestIndex(binary)) {
            testIndex.addDoc("a\0x", "b\0y");
            testIndex.addDoc("c\0y", "d\0z");
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);

            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(new BytesRef("x"), values.nextValue());
            assertEquals(new BytesRef("y"), values.nextValue());

            assertTrue(values.advanceExact(1));
            assertEquals(2, values.docValueCount());
            assertEquals(new BytesRef("y"), values.nextValue());
            assertEquals(new BytesRef("z"), values.nextValue());
        }
    }

    public void testRandomized() throws IOException {
        boolean binary = randomBoolean();
        int docCount = randomIntBetween(1, 20);

        try (TestIndex testIndex = new TestIndex(binary)) {
            List<Set<String>> expectedValuesPerDoc = new ArrayList<>();
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
                testIndex.addDoc(keyedValues);
                expectedValuesPerDoc.add(uniqueValues);
            }
            testIndex.build();

            SortedBinaryDocValues values = testIndex.loadRootValues(0);
            for (int d = 0; d < docCount; d++) {
                assertTrue("doc " + d + " should have values", values.advanceExact(d));
                Set<String> expected = expectedValuesPerDoc.get(d);
                assertEquals("doc " + d + " value count", expected.size(), values.docValueCount());
                Set<String> actual = new HashSet<>();
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(values.nextValue().utf8ToString());
                }
                assertEquals("doc " + d + " values", expected, actual);
            }
        }
    }

    /**
     * Helper that builds a real Lucene index with keyed flattened doc values
     * (either sorted-set or binary format) and provides access to the derived
     * root field data via {@link RootFlattenedFromKeyedFieldData}.
     */
    private static class TestIndex implements AutoCloseable {
        private static final String KEYED_FIELD = "field._keyed";
        private static final String ROOT_FIELD = "field";

        private final boolean useBinaryDocValues;
        private final Directory directory;
        private final RandomIndexWriter writer;
        private DirectoryReader reader;
        private IndexFieldData<?> rootFieldData;

        TestIndex(boolean useBinaryDocValues) throws IOException {
            this.useBinaryDocValues = useBinaryDocValues;
            this.directory = newDirectory();
            if (useBinaryDocValues) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
                this.writer = new RandomIndexWriter(random(), directory, iwc);
            } else {
                this.writer = new RandomIndexWriter(random(), directory);
            }
        }

        void addDoc(String... keyedValues) throws IOException {
            Document doc = new Document();
            if (useBinaryDocValues) {
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

        void addEmptyDoc() throws IOException {
            writer.addDocument(new Document());
        }

        void build() throws IOException {
            writer.flush();
            reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));

            var builder = new RootFlattenedFromKeyedFieldData.Builder(ROOT_FIELD, KEYED_FIELD, useBinaryDocValues);
            rootFieldData = builder.build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        }

        SortedBinaryDocValues loadRootValues(int leafIndex) {
            LeafReaderContext leafCtx = reader.leaves().get(leafIndex);
            LeafFieldData leafData = rootFieldData.load(leafCtx);
            return leafData.getBytesValues();
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
            writer.close();
            directory.close();
        }
    }
}
