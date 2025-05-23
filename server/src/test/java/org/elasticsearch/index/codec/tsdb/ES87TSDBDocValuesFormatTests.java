/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.Elasticsearch816Codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ES87TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private static final int NUM_DOCS = 10;

    static {
        // For Elasticsearch900Lucene101Codec:
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    static class TestES87TSDBDocValuesFormat extends ES87TSDBDocValuesFormat {

        TestES87TSDBDocValuesFormat() {
            super();
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new ES87TSDBDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
        }
    }

    private final Codec codec = new Elasticsearch816Codec() {

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return new TestES87TSDBDocValuesFormat();
        }
    };

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testSortedDocValuesSingleUniqueValue() throws IOException {
        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setCodec(getCodec());
            conf.setMergePolicy(newLogMergePolicy());
            try (RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf)) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    Document doc = new Document();
                    doc.add(new SortedDocValuesField("field", newBytesRef("value")));
                    doc.add(new SortedDocValuesField("field" + i, newBytesRef("value" + i)));
                    iwriter.addDocument(doc);
                }
                iwriter.forceMerge(1);
            }
            try (IndexReader ireader = maybeWrapWithMergingReader(DirectoryReader.open(directory))) {
                assert ireader.leaves().size() == 1;
                SortedDocValues field = ireader.leaves().get(0).reader().getSortedDocValues("field");
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertEquals(i, field.nextDoc());
                    assertEquals(0, field.ordValue());
                    BytesRef scratch = field.lookupOrd(0);
                    assertEquals("value", scratch.utf8ToString());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, field.nextDoc());
                for (int i = 0; i < NUM_DOCS; i++) {
                    SortedDocValues fieldN = ireader.leaves().get(0).reader().getSortedDocValues("field" + i);
                    assertEquals(i, fieldN.nextDoc());
                    assertEquals(0, fieldN.ordValue());
                    BytesRef scratch = fieldN.lookupOrd(0);
                    assertEquals("value" + i, scratch.utf8ToString());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, fieldN.nextDoc());
                }
            }
        }
    }

    public void testSortedSetDocValuesSingleUniqueValue() throws IOException {
        try (Directory directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setCodec(getCodec());
            conf.setMergePolicy(newLogMergePolicy());
            try (RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, conf)) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", newBytesRef("value")));
                    doc.add(new SortedSetDocValuesField("field" + i, newBytesRef("value" + i)));
                    iwriter.addDocument(doc);
                }
                iwriter.forceMerge(1);
            }

            try (IndexReader ireader = maybeWrapWithMergingReader(DirectoryReader.open(directory))) {
                assert ireader.leaves().size() == 1;
                var field = ireader.leaves().get(0).reader().getSortedSetDocValues("field");
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertEquals(i, field.nextDoc());
                    assertEquals(1, field.docValueCount());
                    assertEquals(0, field.nextOrd());
                    BytesRef scratch = field.lookupOrd(0);
                    assertEquals("value", scratch.utf8ToString());
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, field.nextOrd());
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, field.nextDoc());
                for (int i = 0; i < NUM_DOCS; i++) {
                    var fieldN = ireader.leaves().get(0).reader().getSortedSetDocValues("field" + i);
                    assertEquals(i, fieldN.nextDoc());
                    assertEquals(1, fieldN.docValueCount());
                    assertEquals(0, fieldN.nextOrd());
                    BytesRef scratch = fieldN.lookupOrd(0);
                    assertEquals("value" + i, scratch.utf8ToString());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, fieldN.nextDoc());
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, fieldN.nextOrd());
                }
            }
        }
    }

    public void testOneDocManyValues() throws Exception {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(getCodec());
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int numValues = 128 + random().nextInt(1024); // > 2^7 to require two blocks
            Document d = new Document();
            for (int i = 0; i < numValues; i++) {
                d.add(new SortedSetDocValuesField("dv", new BytesRef("v-" + i)));
            }
            writer.addDocument(d);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                SortedSetDocValues dv = leaf.reader().getSortedSetDocValues("dv");
                for (int i = 0; i < 3; i++) {
                    assertTrue(dv.advanceExact(0));
                    assertThat(dv.docValueCount(), equalTo(numValues));
                    for (int v = 0; v < dv.docValueCount(); v++) {
                        assertThat(dv.nextOrd(), greaterThanOrEqualTo(0L));
                    }
                }
            }
        }
    }

    public void testManyDocsWithManyValues() throws Exception {
        final int numDocs = 10 + random().nextInt(20);
        final Map<String, List<String>> sortedSet = new HashMap<>(); // key -> doc-values
        final Map<String, long[]> sortedNumbers = new HashMap<>(); // key -> numbers
        try (Directory directory = newDirectory()) {
            IndexWriterConfig conf = newIndexWriterConfig();
            conf.setCodec(getCodec());
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory, conf)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    String key = "k-" + i;
                    doc.add(new StringField("key", new BytesRef(key), Field.Store.YES));
                    int numValues = random().nextInt(600);
                    List<String> binary = new ArrayList<>();
                    for (int v = 0; v < numValues; v++) {
                        String dv = "v-" + random().nextInt(3) + ":" + v;
                        binary.add(dv);
                        doc.add(new SortedSetDocValuesField("binary", new BytesRef(dv)));
                    }
                    sortedSet.put(key, binary.stream().sorted().toList());
                    numValues = random().nextInt(600);
                    long[] numbers = new long[numValues];
                    for (int v = 0; v < numValues; v++) {
                        numbers[v] = random().nextInt(10) * 1000 + v;
                        doc.add(new SortedNumericDocValuesField("numbers", numbers[v]));
                    }
                    Arrays.sort(numbers);
                    sortedNumbers.put(key, numbers);
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            try (IndexReader reader = maybeWrapWithMergingReader(DirectoryReader.open(directory))) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    StoredFields storedFields = leaf.reader().storedFields();
                    int iters = 1 + random().nextInt(5);
                    for (int i = 0; i < iters; i++) {
                        // check with binary
                        SortedSetDocValues binaryDV = leaf.reader().getSortedSetDocValues("binary");
                        int doc = random().nextInt(leaf.reader().maxDoc());
                        while ((doc = binaryDV.advance(doc)) != DocIdSetIterator.NO_MORE_DOCS) {
                            String key = storedFields.document(doc).getBinaryValue("key").utf8ToString();
                            List<String> expected = sortedSet.get(key);
                            List<String> actual = new ArrayList<>();
                            for (int v = 0; v < binaryDV.docValueCount(); v++) {
                                long ord = binaryDV.nextOrd();
                                actual.add(binaryDV.lookupOrd(ord).utf8ToString());
                            }
                            assertEquals(expected, actual);
                            int repeats = random().nextInt(3);
                            for (int r = 0; r < repeats; r++) {
                                assertTrue(binaryDV.advanceExact(doc));
                                actual.clear();
                                for (int v = 0; v < binaryDV.docValueCount(); v++) {
                                    long ord = binaryDV.nextOrd();
                                    actual.add(binaryDV.lookupOrd(ord).utf8ToString());
                                }
                                assertEquals(expected, actual);
                            }
                            doc++;
                            doc += random().nextInt(3);
                        }
                        // check with numbers
                        doc = random().nextInt(leaf.reader().maxDoc());
                        SortedNumericDocValues numbersDV = leaf.reader().getSortedNumericDocValues("numbers");
                        while ((doc = numbersDV.advance(doc)) != DocIdSetIterator.NO_MORE_DOCS) {
                            String key = storedFields.document(doc).getBinaryValue("key").utf8ToString();
                            long[] expected = sortedNumbers.get(key);
                            long[] actual = new long[expected.length];
                            for (int v = 0; v < numbersDV.docValueCount(); v++) {
                                actual[v] = numbersDV.nextValue();
                            }
                            assertArrayEquals(expected, actual);
                            int repeats = random().nextInt(3);
                            for (int r = 0; r < repeats; r++) {
                                assertTrue(numbersDV.advanceExact(doc));
                                actual = new long[expected.length];
                                for (int v = 0; v < numbersDV.docValueCount(); v++) {
                                    actual[v] = numbersDV.nextValue();
                                }
                                assertArrayEquals(expected, actual);
                            }
                            doc++;
                            doc += random().nextInt(3);
                        }
                    }
                }
            }
        }
    }
}
