/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
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
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.codec.Elasticsearch816Codec;
import org.elasticsearch.index.codec.Elasticsearch900Lucene101Codec;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class TsdbDocValueBwcTests extends ESTestCase {

    public void testMixedIndex() throws Exception {
        var oldCodec = TestUtil.alwaysDocValuesFormat(new TestES87TSDBDocValuesFormat());
        var newCodec = TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat());
        testMixedIndex(oldCodec, newCodec);
    }

    public void testMixedIndex816To900Lucene101() throws Exception {
        var oldCodec = new Elasticsearch816Codec() {

            final DocValuesFormat docValuesFormat = new TestES87TSDBDocValuesFormat();

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        var newCodec = new Elasticsearch900Lucene101Codec() {

            final DocValuesFormat docValuesFormat = new ES819TSDBDocValuesFormat();

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        testMixedIndex(oldCodec, newCodec);
    }

    void testMixedIndex(Codec oldCodec, Codec newCodec) throws IOException, NoSuchFieldException, IllegalAccessException,
        ClassNotFoundException {
        String timestampField = "@timestamp";
        String hostnameField = "host.name";
        long baseTimestamp = 1704067200000L;
        int numRounds = 4 + random().nextInt(8);
        int numDocsPerRound = 64 + random().nextInt(128);
        int numDocs = numRounds * numDocsPerRound;

        try (var dir = newDirectory()) {
            long counter1 = 0;
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };
            try (var iw = new IndexWriter(dir, getTimeSeriesIndexWriterConfig(hostnameField, timestampField, oldCodec))) {
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

                        if (r % 10 < 8) {
                            // Most of the time store counter:
                            d.add(new NumericDocValuesField("counter_1", counter1++));
                        }

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
            }
            // Check documents before force merge:
            try (var reader = DirectoryReader.open(dir)) {
                assertOldDocValuesFormatVersion(reader);
                // Assert per field format field info attributes:
                // (XPerFieldDocValuesFormat must produce the same attributes as PerFieldDocValuesFormat for BWC.
                // Otherwise, doc values fields may disappear)
                for (var leaf : reader.leaves()) {
                    for (var fieldInfo : leaf.reader().getFieldInfos()) {
                        assertThat(fieldInfo.attributes(), Matchers.aMapWithSize(2));
                        assertThat(fieldInfo.attributes(), Matchers.hasEntry("PerFieldDocValuesFormat.suffix", "0"));
                        assertThat(fieldInfo.attributes(), Matchers.hasEntry("PerFieldDocValuesFormat.format", "ES87TSDB"));
                    }
                }

                var hostNameDV = MultiDocValues.getSortedValues(reader, hostnameField);
                assertNotNull(hostNameDV);
                var timestampDV = MultiDocValues.getSortedNumericValues(reader, timestampField);
                assertNotNull(timestampDV);
                var counterOneDV = MultiDocValues.getNumericValues(reader, "counter_1");
                if (counterOneDV == null) {
                    counterOneDV = DocValues.emptyNumeric();
                }
                var gaugeOneDV = MultiDocValues.getSortedNumericValues(reader, "gauge_1");
                if (gaugeOneDV == null) {
                    gaugeOneDV = DocValues.emptySortedNumeric();
                }
                var tagsDV = MultiDocValues.getSortedSetValues(reader, "tags");
                if (tagsDV == null) {
                    tagsDV = DocValues.emptySortedSet();
                }
                for (int i = 0; i < numDocs; i++) {
                    assertEquals(i, hostNameDV.nextDoc());
                    String actualHostName = hostNameDV.lookupOrd(hostNameDV.ordValue()).utf8ToString();
                    assertTrue("unexpected host name:" + actualHostName, actualHostName.startsWith("host-"));

                    assertEquals(i, timestampDV.nextDoc());
                    long timestamp = timestampDV.nextValue();
                    long lowerBound = baseTimestamp;
                    long upperBound = baseTimestamp + numDocs;
                    assertTrue(
                        "unexpected timestamp [" + timestamp + "], expected between [" + lowerBound + "] and [" + upperBound + "]",
                        timestamp >= lowerBound && timestamp < upperBound
                    );
                    if (counterOneDV.advanceExact(i)) {
                        long counterOneValue = counterOneDV.longValue();
                        assertTrue("unexpected counter [" + counterOneValue + "]", counterOneValue >= 0 && counterOneValue < counter1);
                    }
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

            var iwc = getTimeSeriesIndexWriterConfig(hostnameField, timestampField, newCodec);
            iwc.setMergePolicy(new LogByteSizeMergePolicy());
            try (var iw = new IndexWriter(dir, iwc)) {
                iw.forceMerge(1);
                // Check documents after force merge:
                try (var reader = DirectoryReader.open(iw)) {
                    assertEquals(1, reader.leaves().size());
                    assertEquals(numDocs, reader.maxDoc());
                    assertNewDocValuesFormatVersion(reader);
                    var leaf = reader.leaves().get(0).reader();
                    // Assert per field format field info attributes:
                    // (XPerFieldDocValuesFormat must produce the same attributes as PerFieldDocValuesFormat for BWC.
                    // Otherwise, doc values fields may disappear)
                    for (var fieldInfo : leaf.getFieldInfos()) {
                        assertThat(fieldInfo.attributes(), Matchers.aMapWithSize(2));
                        assertThat(fieldInfo.attributes(), Matchers.hasEntry("PerFieldDocValuesFormat.suffix", "0"));
                        assertThat(fieldInfo.attributes(), Matchers.hasEntry("PerFieldDocValuesFormat.format", "ES819TSDB"));
                    }

                    var hostNameDV = leaf.getSortedDocValues(hostnameField);
                    assertNotNull(hostNameDV);
                    var timestampDV = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(timestampField));
                    assertNotNull(timestampDV);
                    var counterOneDV = leaf.getNumericDocValues("counter_1");
                    if (counterOneDV == null) {
                        counterOneDV = DocValues.emptyNumeric();
                    }
                    var gaugeOneDV = leaf.getSortedNumericDocValues("gauge_1");
                    if (gaugeOneDV == null) {
                        gaugeOneDV = DocValues.emptySortedNumeric();
                    }
                    var tagsDV = leaf.getSortedSetDocValues("tags");
                    if (tagsDV == null) {
                        tagsDV = DocValues.emptySortedSet();
                    }
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
                        if (counterOneDV.advanceExact(i)) {
                            long counterOneValue = counterOneDV.longValue();
                            assertTrue("unexpected counter [" + counterOneValue + "]", counterOneValue >= 0 && counterOneValue < counter1);
                        }
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
    }

    private IndexWriterConfig getTimeSeriesIndexWriterConfig(String hostnameField, String timestampField, Codec codec) {
        var config = new IndexWriterConfig();
        config.setIndexSort(
            new Sort(
                new SortField(hostnameField, SortField.Type.STRING, false),
                new SortedNumericSortField(timestampField, SortField.Type.LONG, true)
            )
        );
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        // avoids the usage of ES87TSDBDocValuesProducer while indexing using old codec:
        // (The per field format encodes the dv codec name and that then loads the current dv codec)
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setCodec(codec);
        return config;
    }

    // A hacky way to figure out whether doc values format is written in what version. Need to use reflection, because
    // PerFieldDocValuesFormat hides the doc values formats it wraps.
    private void assertOldDocValuesFormatVersion(DirectoryReader reader) throws NoSuchFieldException, IllegalAccessException, IOException {
        if (System.getSecurityManager() != null) {
            // With jvm version 24 entitlements are used and security manager is nog longer used.
            // Making this assertion work with security manager requires granting the entire test codebase privileges to use
            // suppressAccessChecks and accessDeclaredMembers. This is undesired from a security manager perspective.
            logger.info("not asserting doc values format version, because security manager is used");
            return;
        }

        for (var leafReaderContext : reader.leaves()) {
            var leaf = (SegmentReader) leafReaderContext.reader();
            var dvReader = leaf.getDocValuesReader();
            var field = getFormatsFieldFromPerFieldFieldsReader(dvReader.getClass());
            Map<?, ?> formats = (Map<?, ?>) field.get(dvReader);
            assertThat(formats, Matchers.aMapWithSize(1));
            var tsdbDvReader = (DocValuesProducer) formats.get("ES87TSDB_0");
            tsdbDvReader.checkIntegrity();
            assertThat(tsdbDvReader, Matchers.instanceOf(ES87TSDBDocValuesProducer.class));
        }
    }

    private void assertNewDocValuesFormatVersion(DirectoryReader reader) throws NoSuchFieldException, IllegalAccessException, IOException,
        ClassNotFoundException {

        for (var leafReaderContext : reader.leaves()) {
            var leaf = (SegmentReader) leafReaderContext.reader();
            var dvReader = leaf.getDocValuesReader();
            dvReader.checkIntegrity();

            if (dvReader instanceof XPerFieldDocValuesFormat.FieldsReader perFieldDvReader) {
                var formats = perFieldDvReader.getFormats();
                assertThat(formats, Matchers.aMapWithSize(1));
                var tsdbDvReader = formats.get("ES819TSDB_0");
                tsdbDvReader.checkIntegrity();
                assertThat(
                    tsdbDvReader,
                    Matchers.instanceOf(Class.forName("org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesProducer"))
                );
            } else {
                if (System.getSecurityManager() != null) {
                    // With jvm version 24 entitlements are used and security manager is nog longer used.
                    // Making this assertion work with security manager requires granting the entire test codebase privileges to use
                    // suppressAccessChecks and suppressAccessChecks. This is undesired from a security manager perspective.
                    logger.info("not asserting doc values format version, because security manager is used");
                    continue;
                }
                var field = getFormatsFieldFromPerFieldFieldsReader(dvReader.getClass());
                Map<?, ?> formats = (Map<?, ?>) field.get(dvReader);
                assertThat(formats, Matchers.aMapWithSize(1));
                var tsdbDvReader = (DocValuesProducer) formats.get("ES819TSDB_0");
                tsdbDvReader.checkIntegrity();
                assertThat(
                    tsdbDvReader,
                    Matchers.instanceOf(Class.forName("org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesProducer"))
                );
            }
        }
    }

    @SuppressForbidden(reason = "access violation required in order to read private field for this test")
    private static Field getFormatsFieldFromPerFieldFieldsReader(Class<?> c) throws NoSuchFieldException {
        var field = c.getDeclaredField("formats");
        field.setAccessible(true);
        return field;
    }

}
