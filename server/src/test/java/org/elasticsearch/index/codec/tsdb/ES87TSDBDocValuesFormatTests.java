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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ES87TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private static final int NUM_DOCS = 10;

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormat());

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

    public void testForceMergeDenseCase() throws Exception {
        String timestampField = "@timestamp";
        String hostnameField = "host.name";
        long baseTimestamp = 1704067200000L;

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long counter1 = 0;
            long counter2 = 10_000_000;
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            long[] gauge2Values = new long[] { -2, -4, -6, -8, -10, -12, -14, -16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };

            int numDocs = 256 + random().nextInt(1024);
            int numHosts = numDocs / 20;
            for (int i = 0; i < numDocs; i++) {
                var d = new Document();

                int batchIndex = i / numHosts;
                String hostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                long timestamp = baseTimestamp + (1000L * i);

                d.add(new SortedDocValuesField(hostnameField, new BytesRef(hostName)));
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(new SortedNumericDocValuesField(timestampField, timestamp));
                d.add(new NumericDocValuesField("counter_1", counter1++));
                d.add(new SortedNumericDocValuesField("counter_2", counter2++));
                d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[i % gauge1Values.length]));
                d.add(new SortedNumericDocValuesField("gauge_2", gauge2Values[i % gauge1Values.length]));
                int numTags = 1 + random().nextInt(8);
                for (int j = 0; j < numTags; j++) {
                    d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[j])));
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
            }
            iw.commit();

            iw.forceMerge(1);

            // For asserting using binary search later on:
            Arrays.sort(gauge2Values);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().get(0).reader();
                var hostNameDV = leaf.getSortedDocValues(hostnameField);
                assertNotNull(hostNameDV);
                var timestampDV = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(timestampField));
                assertNotNull(timestampDV);
                var counterOneDV = leaf.getNumericDocValues("counter_1");
                assertNotNull(counterOneDV);
                var counterTwoDV = leaf.getSortedNumericDocValues("counter_2");
                assertNotNull(counterTwoDV);
                var gaugeOneDV = leaf.getSortedNumericDocValues("gauge_1");
                assertNotNull(gaugeOneDV);
                var gaugeTwoDV = leaf.getSortedNumericDocValues("gauge_2");
                assertNotNull(gaugeTwoDV);
                var tagsDV = leaf.getSortedSetDocValues("tags");
                assertNotNull(tagsDV);
                for (int i = 0; i < numDocs; i++) {
                    assertEquals(i, hostNameDV.nextDoc());
                    int batchIndex = i / numHosts;
                    assertEquals(batchIndex, hostNameDV.ordValue());
                    String expectedHostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                    assertEquals(expectedHostName, hostNameDV.lookupOrd(hostNameDV.ordValue()).utf8ToString());

                    assertEquals(i, timestampDV.nextDoc());
                    long timestamp = timestampDV.longValue();
                    long lowerBound = baseTimestamp;
                    long upperBound = baseTimestamp + (1000L * numDocs);
                    assertTrue(
                        "unexpected timestamp [" + timestamp + "], expected between [" + lowerBound + "] and [" + upperBound + "]",
                        timestamp >= lowerBound && timestamp < upperBound
                    );

                    assertEquals(i, counterOneDV.nextDoc());
                    long counterOneValue = counterOneDV.longValue();
                    assertTrue("unexpected counter [" + counterOneValue + "]", counterOneValue >= 0 && counterOneValue < counter1);

                    assertEquals(i, counterTwoDV.nextDoc());
                    assertEquals(1, counterTwoDV.docValueCount());
                    long counterTwoValue = counterTwoDV.nextValue();
                    assertTrue("unexpected counter [" + counterTwoValue + "]", counterTwoValue > 0 && counterTwoValue <= counter2);

                    assertEquals(i, gaugeOneDV.nextDoc());
                    assertEquals(1, gaugeOneDV.docValueCount());
                    long gaugeOneValue = gaugeOneDV.nextValue();
                    assertTrue("unexpected gauge [" + gaugeOneValue + "]", Arrays.binarySearch(gauge1Values, gaugeOneValue) >= 0);

                    assertEquals(i, gaugeTwoDV.nextDoc());
                    assertEquals(1, gaugeTwoDV.docValueCount());
                    long gaugeTwoValue = gaugeTwoDV.nextValue();
                    assertTrue("unexpected gauge [" + gaugeTwoValue + "]", Arrays.binarySearch(gauge2Values, gaugeTwoValue) >= 0);

                    assertEquals(i, tagsDV.nextDoc());
                    for (int j = 0; j < tagsDV.docValueCount(); j++) {
                        long ordinal = tagsDV.nextOrd();
                        String actualTag = tagsDV.lookupOrd(ordinal).utf8ToString();
                        assertTrue("unexpected tag [" + actualTag + "]", Arrays.binarySearch(tags, actualTag) >= 0);
                    }
                }
            }
        }
    }

    public void testWithNoValueMultiValue() throws Exception {
        String timestampField = "@timestamp";
        String hostnameField = "host.name";
        long baseTimestamp = 1704067200000L;

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int numRounds = 4 + random().nextInt(28);
            int numDocsPerRound = 8 + random().nextInt(56);
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };
            {
                long timestamp = baseTimestamp;
                for (int i = 0; i < numRounds; i++) {
                    int r = random().nextInt(10);
                    for (int j = 0; j < numDocsPerRound; j++) {
                        var d = new Document();
                        String hostName = String.format(Locale.ROOT, "host-%03d", i);
                        d.add(new SortedDocValuesField(hostnameField, new BytesRef(hostName)));
                        // Index sorting doesn't work with NumericDocValuesField:
                        d.add(new SortedNumericDocValuesField(timestampField, timestamp++));

                        if (r % 10 == 5) {
                            // sometimes no values
                        } else if (r % 10 > 5) {
                            // often multiple values:
                            int numValues = 2 + random().nextInt(4);
                            for (int k = 0; k < numValues; k++) {
                                d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[(j + k) % gauge1Values.length]));
                                d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[(j + k) % tags.length])));
                            }
                        } else {
                            // otherwise single value:
                            d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[j % gauge1Values.length]));
                            d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[j % tags.length])));
                        }

                        iw.addDocument(d);
                    }
                    iw.commit();
                }
                iw.forceMerge(1);
            }

            int numDocs = numRounds * numDocsPerRound;
            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().get(0).reader();
                var hostNameDV = leaf.getSortedDocValues(hostnameField);
                assertNotNull(hostNameDV);
                var timestampDV = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(timestampField));
                assertNotNull(timestampDV);
                var gaugeOneDV = leaf.getSortedNumericDocValues("gauge_1");
                assertNotNull(gaugeOneDV);
                var tagsDV = leaf.getSortedSetDocValues("tags");
                assertNotNull(tagsDV);
                for (int i = 0; i < numDocs; i++) {
                    assertEquals(i, hostNameDV.nextDoc());
                    int round = i / numDocsPerRound;
                    String expectedHostName = String.format(Locale.ROOT, "host-%03d", round);
                    String actualHostName = hostNameDV.lookupOrd(hostNameDV.ordValue()).utf8ToString();
                    assertEquals(expectedHostName, actualHostName);

                    assertEquals(i, timestampDV.nextDoc());
                    long timestamp = timestampDV.longValue();
                    long lowerBound = baseTimestamp;
                    long upperBound = baseTimestamp + numDocs;
                    assertTrue(
                        "unexpected timestamp [" + timestamp + "], expected between [" + lowerBound + "] and [" + upperBound + "]",
                        timestamp >= lowerBound && timestamp < upperBound
                    );
                    if (gaugeOneDV.advanceExact(i)) {
                        for (int j = 0; j < gaugeOneDV.docValueCount(); j++) {
                            long value = gaugeOneDV.nextValue();
                            assertTrue("unexpected gauge [" + value + "]", Arrays.binarySearch(gauge1Values, value) >= 0);
                        }
                    }
                    if (tagsDV.advanceExact(i)) {
                        for (int j = 0; j < tagsDV.docValueCount(); j++) {
                            long ordinal = tagsDV.nextOrd();
                            String actualTag = tagsDV.lookupOrd(ordinal).utf8ToString();
                            assertTrue("unexpected tag [" + actualTag + "]", Arrays.binarySearch(tags, actualTag) >= 0);
                        }
                    }
                }
            }
        }
    }

    private IndexWriterConfig getTimeSeriesIndexWriterConfig(String hostnameField, String timestampField) {
        var config = new IndexWriterConfig();
        config.setIndexSort(
            new Sort(
                new SortField(hostnameField, SortField.Type.STRING, false),
                new SortedNumericSortField(timestampField, SortField.Type.LONG, true)
            )
        );
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(getCodec());
        return config;
    }
}
