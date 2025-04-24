/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.codec.Elasticsearch900Lucene101Codec;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests;

import java.util.Arrays;
import java.util.Locale;

public class ES819TSDBDocValuesFormatTests extends ES87TSDBDocValuesFormatTests {

    private final Codec codec = new Elasticsearch900Lucene101Codec() {

        final ES819TSDBDocValuesFormat docValuesFormat = new ES819TSDBDocValuesFormat();

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    @Override
    protected Codec getCodec() {
        return codec;
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

                int numGauge2 = 1 + random().nextInt(8);
                for (int j = 0; j < numGauge2; j++) {
                    d.add(new SortedNumericDocValuesField("gauge_2", gauge2Values[(i + j) % gauge2Values.length]));
                }

                int numTags = 1 + random().nextInt(8);
                for (int j = 0; j < numTags; j++) {
                    d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[(i + j) % tags.length])));
                }

                d.add(new BinaryDocValuesField("tags_as_bytes", new BytesRef(tags[i % tags.length])));

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
                var tagBytesDV = leaf.getBinaryDocValues("tags_as_bytes");
                assertNotNull(tagBytesDV);
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
                    for (int j = 0; j < gaugeTwoDV.docValueCount(); j++) {
                        long gaugeTwoValue = gaugeTwoDV.nextValue();
                        assertTrue("unexpected gauge [" + gaugeTwoValue + "]", Arrays.binarySearch(gauge2Values, gaugeTwoValue) >= 0);
                    }

                    assertEquals(i, tagsDV.nextDoc());
                    for (int j = 0; j < tagsDV.docValueCount(); j++) {
                        long ordinal = tagsDV.nextOrd();
                        String actualTag = tagsDV.lookupOrd(ordinal).utf8ToString();
                        assertTrue("unexpected tag [" + actualTag + "]", Arrays.binarySearch(tags, actualTag) >= 0);
                    }

                    assertEquals(i, tagBytesDV.nextDoc());
                    BytesRef tagBytesValue = tagBytesDV.binaryValue();
                    assertTrue("unexpected bytes " + tagBytesValue, Arrays.binarySearch(tags, tagBytesValue.utf8ToString()) >= 0);
                }
            }
        }
    }

    public void testTwoSegmentsTwoDifferentFields() throws Exception {
        String timestampField = "@timestamp";
        String hostnameField = "host.name";
        long timestamp = 1704067200000L;

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long counter1 = 0;
            long counter2 = 10_000_000;

            {
                var d = new Document();
                d.add(new SortedDocValuesField(hostnameField, new BytesRef("host-001")));
                d.add(new SortedNumericDocValuesField(timestampField, timestamp - 1));
                d.add(new NumericDocValuesField("counter_1", counter1));
                d.add(new SortedNumericDocValuesField("gauge_1", 2));
                d.add(new BinaryDocValuesField("binary_1", new BytesRef("foo")));
                iw.addDocument(d);
                iw.commit();
            }
            {
                var d = new Document();
                d.add(new SortedDocValuesField(hostnameField, new BytesRef("host-001")));
                d.add(new SortedNumericDocValuesField(timestampField, timestamp));
                d.add(new SortedNumericDocValuesField("counter_2", counter2));
                d.add(new SortedNumericDocValuesField("gauge_2", -2));
                d.add(new BinaryDocValuesField("binary_2", new BytesRef("bar")));
                iw.addDocument(d);
                iw.commit();
            }

            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(2, reader.maxDoc());
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
                var binaryOneDV = leaf.getBinaryDocValues("binary_1");
                assertNotNull(binaryOneDV);
                var binaryTwoDv = leaf.getBinaryDocValues("binary_2");
                assertNotNull(binaryTwoDv);
                for (int i = 0; i < 2; i++) {
                    assertEquals(i, hostNameDV.nextDoc());
                    assertEquals("host-001", hostNameDV.lookupOrd(hostNameDV.ordValue()).utf8ToString());

                    assertEquals(i, timestampDV.nextDoc());
                    long actualTimestamp = timestampDV.longValue();
                    assertTrue(actualTimestamp == timestamp || actualTimestamp == timestamp - 1);

                    if (counterOneDV.advanceExact(i)) {
                        long counterOneValue = counterOneDV.longValue();
                        assertEquals(counter1, counterOneValue);
                    }

                    if (counterTwoDV.advanceExact(i)) {
                        assertEquals(1, counterTwoDV.docValueCount());
                        long counterTwoValue = counterTwoDV.nextValue();
                        assertEquals(counter2, counterTwoValue);
                    }

                    if (gaugeOneDV.advanceExact(i)) {
                        assertEquals(1, gaugeOneDV.docValueCount());
                        long gaugeOneValue = gaugeOneDV.nextValue();
                        assertEquals(2, gaugeOneValue);
                    }

                    if (gaugeTwoDV.advanceExact(i)) {
                        assertEquals(1, gaugeTwoDV.docValueCount());
                        long gaugeTwoValue = gaugeTwoDV.nextValue();
                        assertEquals(-2, gaugeTwoValue);
                    }

                    if (binaryOneDV.advanceExact(i)) {
                        BytesRef binaryOneValue = binaryOneDV.binaryValue();
                        assertEquals(new BytesRef("foo"), binaryOneValue);
                    }

                    if (binaryTwoDv.advanceExact(i)) {
                        BytesRef binaryTwoValue = binaryTwoDv.binaryValue();
                        assertEquals(new BytesRef("bar"), binaryTwoValue);
                    }
                }
            }
        }
    }

    public void testForceMergeSparseCase() throws Exception {
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

                if (random().nextBoolean()) {
                    d.add(new NumericDocValuesField("counter_1", counter1++));
                }
                if (random().nextBoolean()) {
                    d.add(new SortedNumericDocValuesField("counter_2", counter2++));
                }
                if (random().nextBoolean()) {
                    d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[i % gauge1Values.length]));
                }
                if (random().nextBoolean()) {
                    int numGauge2 = 1 + random().nextInt(8);
                    for (int j = 0; j < numGauge2; j++) {
                        d.add(new SortedNumericDocValuesField("gauge_2", gauge2Values[(i + j) % gauge2Values.length]));
                    }
                }
                if (random().nextBoolean()) {
                    int numTags = 1 + random().nextInt(8);
                    for (int j = 0; j < numTags; j++) {
                        d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[j])));
                    }
                }
                if (random().nextBoolean()) {
                    int randomIndex = random().nextInt(tags.length);
                    d.add(new SortedDocValuesField("other_tag", new BytesRef(tags[randomIndex])));
                }
                if (random().nextBoolean()) {
                    int randomIndex = random().nextInt(tags.length);
                    d.add(new BinaryDocValuesField("tags_as_bytes", new BytesRef(tags[randomIndex])));
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
                var otherTagDV = leaf.getSortedDocValues("other_tag");
                assertNotNull(otherTagDV);
                var tagBytesDV = leaf.getBinaryDocValues("tags_as_bytes");
                assertNotNull(tagBytesDV);
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

                    if (counterOneDV.advanceExact(i)) {
                        long counterOneValue = counterOneDV.longValue();
                        assertTrue("unexpected counter [" + counterOneValue + "]", counterOneValue >= 0 && counterOneValue < counter1);
                    }

                    if (counterTwoDV.advanceExact(i)) {
                        assertEquals(1, counterTwoDV.docValueCount());
                        long counterTwoValue = counterTwoDV.nextValue();
                        assertTrue("unexpected counter [" + counterTwoValue + "]", counterTwoValue > 0 && counterTwoValue <= counter2);
                    }

                    if (gaugeOneDV.advanceExact(i)) {
                        assertEquals(1, gaugeOneDV.docValueCount());
                        long gaugeOneValue = gaugeOneDV.nextValue();
                        assertTrue("unexpected gauge [" + gaugeOneValue + "]", Arrays.binarySearch(gauge1Values, gaugeOneValue) >= 0);
                    }

                    if (gaugeTwoDV.advanceExact(i)) {
                        for (int j = 0; j < gaugeTwoDV.docValueCount(); j++) {
                            long gaugeTwoValue = gaugeTwoDV.nextValue();
                            assertTrue("unexpected gauge [" + gaugeTwoValue + "]", Arrays.binarySearch(gauge2Values, gaugeTwoValue) >= 0);
                        }
                    }

                    if (tagsDV.advanceExact(i)) {
                        for (int j = 0; j < tagsDV.docValueCount(); j++) {
                            long ordinal = tagsDV.nextOrd();
                            String actualTag = tagsDV.lookupOrd(ordinal).utf8ToString();
                            assertTrue("unexpected tag [" + actualTag + "]", Arrays.binarySearch(tags, actualTag) >= 0);
                        }
                    }
                    if (otherTagDV.advanceExact(i)) {
                        int ordinal = otherTagDV.ordValue();
                        String actualTag = otherTagDV.lookupOrd(ordinal).utf8ToString();
                        assertTrue("unexpected tag [" + actualTag + "]", Arrays.binarySearch(tags, actualTag) >= 0);
                    }

                    if (tagBytesDV.advanceExact(i)) {
                        BytesRef tagBytesValue = tagBytesDV.binaryValue();
                        assertTrue("unexpected bytes " + tagBytesValue, Arrays.binarySearch(tags, tagBytesValue.utf8ToString()) >= 0);
                    }
                }
            }
        }
    }

    public void testWithNoValueMultiValue() throws Exception {
        String timestampField = "@timestamp";
        String hostnameField = "host.name";
        long baseTimestamp = 1704067200000L;
        int numRounds = 32 + random().nextInt(32);
        int numDocsPerRound = 64 + random().nextInt(64);

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };
            {
                long timestamp = baseTimestamp;
                for (int i = 0; i < numRounds; i++) {
                    int r = random().nextInt(10);
                    for (int j = 0; j < numDocsPerRound; j++) {
                        var d = new Document();
                        // host in reverse, otherwise merging will detect that segments are already ordered and will use sequential docid
                        // merger:
                        String hostName = String.format(Locale.ROOT, "host-%03d", numRounds - i);
                        d.add(new SortedDocValuesField(hostnameField, new BytesRef(hostName)));
                        // Index sorting doesn't work with NumericDocValuesField:
                        d.add(new SortedNumericDocValuesField(timestampField, timestamp++));

                        if (r % 10 == 5) {
                            // sometimes no values
                        } else if (r % 10 > 5) {
                            // often single value:
                            d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[j % gauge1Values.length]));
                            d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[j % tags.length])));
                        } else {
                            // otherwise multiple values:
                            int numValues = 2 + random().nextInt(4);
                            for (int k = 0; k < numValues; k++) {
                                d.add(new SortedNumericDocValuesField("gauge_1", gauge1Values[(j + k) % gauge1Values.length]));
                                d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[(j + k) % tags.length])));
                            }
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
                    String actualHostName = hostNameDV.lookupOrd(hostNameDV.ordValue()).utf8ToString();
                    assertTrue("unexpected host name:" + actualHostName, actualHostName.startsWith("host-"));

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
