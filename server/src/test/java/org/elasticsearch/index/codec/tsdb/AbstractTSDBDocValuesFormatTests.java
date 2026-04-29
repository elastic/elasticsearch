/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.codec.Elasticsearch900Lucene101Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.BaseDenseNumericValues;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.BaseSortedDocValues;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.TSDBBinaryDocValues;
import org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoader.OptionalColumnAtATimeReader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.CustomBinaryDocValuesReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Shared test suite for TSDB doc values formats. Tests all five doc value types (numeric,
 * sorted-numeric, sorted, sorted-set, binary), merge correctness (dense, sparse, mixed),
 * block loader column-at-a-time reading, length/contains iterators, and doc ID run encoding.
 *
 * <p>Subclasses provide codec configuration by overriding {@link #getCodec()}. This gives
 * full test coverage (~160+ inherited Lucene tests plus ~30 TSDB-specific tests) with a
 * single subclass per codec version.
 *
 * <p>Example:
 * <pre>{@code
 * public class ES94TSDBDocValuesFormatTests extends AbstractTSDBDocValuesFormatTests {
 *     private final Codec codec = ... // ES94 codec setup
 *
 *     @Override
 *     protected Codec getCodec() { return codec; }
 * }
 * }</pre>
 */
public abstract class AbstractTSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    protected static final String TIMESTAMP_FIELD = "@timestamp";
    protected static final String HOSTNAME_FIELD = "host.name";
    protected static final long BASE_TIMESTAMP = 1704067200000L;

    protected static final int BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT = 128 * 1024;
    protected static final int BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT = 1024;

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    protected IndexWriterConfig getTimeSeriesIndexWriterConfig(String hostnameField, String timestampField) {
        return getTimeSeriesIndexWriterConfig(hostnameField, false, timestampField);
    }

    protected IndexWriterConfig getTimeSeriesIndexWriterConfig(String hostnameField, boolean multiValued, String timestampField) {
        var config = new IndexWriterConfig();
        if (hostnameField != null) {
            config.setIndexSort(
                new Sort(
                    multiValued ? new SortedSetSortField(hostnameField, false) : new SortField(hostnameField, SortField.Type.STRING, false),
                    new SortedNumericSortField(timestampField, SortField.Type.LONG, true)
                )
            );
        } else {
            config.setIndexSort(new Sort(new SortedNumericSortField(timestampField, SortField.Type.LONG, true)));
        }
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(getCodec());
        return config;
    }

    protected static TSDBBinaryDocValues getTSDBBinaryValues(LeafReader leafReader, String field) throws IOException {
        return (TSDBBinaryDocValues) leafReader.getBinaryDocValues(field);
    }

    protected static BaseDenseNumericValues getBaseDenseNumericValues(LeafReader leafReader, String field) throws IOException {
        return (BaseDenseNumericValues) DocValues.unwrapSingleton(leafReader.getSortedNumericDocValues(field));
    }

    protected static BaseSortedDocValues getBaseSortedDocValues(LeafReader leafReader, String field) throws IOException {
        var sortedDocValues = leafReader.getSortedDocValues(field);
        if (sortedDocValues == null) {
            sortedDocValues = DocValues.unwrapSingleton(leafReader.getSortedSetDocValues(field));
        }
        return (BaseSortedDocValues) sortedDocValues;
    }

    public void testBlockWiseBinary() throws Exception {
        boolean sparse = randomBoolean();
        int numBlocksBound = 10;
        int numNonNullValues = randomIntBetween(1, numBlocksBound * BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT);

        List<String> binaryValues = new ArrayList<>();
        int numNonNull = 0;
        while (numNonNull < numNonNullValues) {
            if (sparse && randomBoolean()) {
                binaryValues.add(null);
            } else {
                final String value = randomAlphaOfLengthBetween(0, 50);
                binaryValues.add(value);
                numNonNull++;
            }
        }

        assertBinaryValues(binaryValues);
    }

    public void testBlockWiseBinarySmallValues() throws Exception {
        boolean sparse = randomBoolean();
        int numBlocksBound = 5;
        int numNonNullValues = randomIntBetween(1, numBlocksBound * BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT);

        List<String> binaryValues = new ArrayList<>();
        int numNonNull = 0;
        while (numNonNull < numNonNullValues) {
            if (sparse && randomBoolean()) {
                binaryValues.add(null);
            } else {
                final String value = randomAlphaOfLengthBetween(0, 2);
                binaryValues.add(value);
                numNonNull++;
            }
        }

        assertBinaryValues(binaryValues);
    }

    public void testBlockWiseBinaryWithEmptySequences() throws Exception {
        List<String> binaryValues = new ArrayList<>();
        int numSequences = 10;
        for (int i = 0; i < numSequences; i++) {
            int numInSequence = randomIntBetween(1, 3 * BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT);
            boolean emptySequence = randomBoolean();
            for (int j = 0; j < numInSequence; j++) {
                binaryValues.add(emptySequence ? "" : randomAlphaOfLengthBetween(0, 5));
            }
        }
        assertBinaryValues(binaryValues);
    }

    public void testBlockWiseBinaryLargeValues() throws Exception {
        boolean sparse = randomBoolean();
        int numBlocksBound = 5;
        int binaryDataSize = randomIntBetween(1, numBlocksBound * BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT);
        List<String> binaryValues = new ArrayList<>();
        int totalSize = 0;
        while (totalSize < binaryDataSize) {
            if (sparse && randomBoolean()) {
                binaryValues.add(null);
            } else {
                final String value = randomAlphaOfLengthBetween(
                    BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT / 2,
                    2 * BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT
                );
                binaryValues.add(value);
                totalSize += value.length();
            }
        }

        assertBinaryValues(binaryValues);
    }

    void assertBinaryValues(List<String> binaryValues) throws Exception {
        assert binaryValues.isEmpty() == false : "binaryValues must be non-empty";
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        String binaryField = "binary_field";
        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {

            int numDocs = binaryValues.size();
            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = BASE_TIMESTAMP + (1000L * i);
                d.add(new SortedDocValuesField(hostnameField, new BytesRef("host-1")));
                d.add(new SortedNumericDocValuesField(timestampField, timestamp));

                String binaryValue = binaryValues.get(i);
                if (binaryValue != null) {
                    d.add(new BinaryDocValuesField(binaryField, new BytesRef(binaryValue)));
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().getFirst().reader();
                var binaryDV = leaf.getBinaryDocValues(binaryField);
                assertNotNull(binaryDV);
                for (int i = 0; i < numDocs; i++) {
                    String expected = binaryValues.removeLast();
                    if (expected == null) {
                        assertFalse(binaryDV.advanceExact(i));
                    } else {
                        assertTrue(binaryDV.advanceExact(i));
                        assertEquals(expected, binaryDV.binaryValue().utf8ToString());
                    }
                }
            }
        }
    }

    public void testForceMergeDenseCase() throws Exception {
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        long lowerBound = BASE_TIMESTAMP;

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
                long timestamp = lowerBound + (1000L * i);

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
                var leaf = reader.leaves().getFirst().reader();
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
                    long upperBound = lowerBound + (1000L * numDocs);
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
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        long timestamp = BASE_TIMESTAMP;
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
                var leaf = reader.leaves().getFirst().reader();
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
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        long lowerBound = BASE_TIMESTAMP;

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
                long timestamp = lowerBound + (1000L * i);

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
                var leaf = reader.leaves().getFirst().reader();
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
                    long upperBound = lowerBound + (1000L * numDocs);
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
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        long lowerBound = BASE_TIMESTAMP;
        int numRounds = 32 + random().nextInt(32);
        int numDocsPerRound = 64 + random().nextInt(64);

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };
            {
                long timestamp = lowerBound;
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
                var leaf = reader.leaves().getFirst().reader();
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
                    long upperBound = lowerBound + numDocs;
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

    public void testOptionalLengthReaderDenseToLengthValues() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFixedLength = randomIntBetween(0, 100);
        long currentTimestamp = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            List<BytesRef> binaryVariableValues = new ArrayList<>();
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));

                binaryVariableValues.add(new BytesRef(randomAlphaOfLength(between(0, 100))));
                d.add(new BinaryDocValuesField(binaryFixedField, new BytesRef(randomAlphaOfLength(binaryFixedLength))));
                d.add(new BinaryDocValuesField(binaryVariableField, binaryVariableValues.getLast()));

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000L;
                }
            }
            iw.commit();

            try (var reader = DirectoryReader.open(iw)) {
                for (var leaf : reader.leaves()) {
                    var binaryFixedDV = getTSDBBinaryValues(leaf.reader(), binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leaf.reader(), binaryVariableField);

                    NumericDocValues fixedLengthReader = binaryFixedDV.toLengthValues();
                    NumericDocValues variableLengthReader = binaryVariableDV.toLengthValues();

                    int maxDoc = leaf.reader().maxDoc();
                    for (int i = 0; i < maxDoc; i++) {
                        assertTrue(fixedLengthReader.advanceExact(i));
                        assertEquals(binaryFixedLength, fixedLengthReader.longValue());

                        assertTrue(variableLengthReader.advanceExact(i));
                        assertEquals(binaryVariableValues.removeLast().length, variableLengthReader.longValue());
                    }
                }
            }
        }
    }

    public void testOptionalLengthReaderSparseToLengthValues() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFixedLength = randomIntBetween(0, 100);
        long currentTimestamp = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            List<BytesRef> binaryVariableValues = new ArrayList<>();
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));

                if (randomBoolean()) {
                    binaryVariableValues.add(new BytesRef(randomAlphaOfLength(between(0, 100))));
                    d.add(new BinaryDocValuesField(binaryFixedField, new BytesRef(randomAlphaOfLength(binaryFixedLength))));
                    d.add(new BinaryDocValuesField(binaryVariableField, binaryVariableValues.getLast()));
                } else {
                    binaryVariableValues.add(null);
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000L;
                }
            }
            iw.commit();

            try (var reader = DirectoryReader.open(iw)) {
                for (var leaf : reader.leaves()) {
                    var binaryFixedDV = getTSDBBinaryValues(leaf.reader(), binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leaf.reader(), binaryVariableField);

                    int maxDoc = leaf.reader().maxDoc();
                    // No docs in this segment had these fields, so doc values are null
                    if (binaryFixedDV == null) {
                        assertNull(binaryVariableDV);
                        for (int i = 0; i < maxDoc; i++) {
                            assertNull(binaryVariableValues.removeLast());
                        }
                        continue;
                    }

                    NumericDocValues fixedLengthReader = binaryFixedDV.toLengthValues();
                    NumericDocValues variableLengthReader = binaryVariableDV.toLengthValues();

                    for (int i = 0; i < maxDoc; i++) {
                        BytesRef expectedVariableLength = binaryVariableValues.removeLast();
                        if (expectedVariableLength == null) {
                            assertFalse(fixedLengthReader.advanceExact(i));
                            assertFalse(variableLengthReader.advanceExact(i));
                        } else {
                            assertTrue(fixedLengthReader.advanceExact(i));
                            assertEquals(binaryFixedLength, fixedLengthReader.longValue());

                            assertTrue(variableLengthReader.advanceExact(i));
                            assertEquals(expectedVariableLength.length, variableLengthReader.longValue());
                        }
                    }
                }
            }
        }
    }

    public void testOptionalLengthReaderDense() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFixedLength = randomIntBetween(0, 100);
        long currentTimestamp = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            List<BytesRef> binaryVariableValues = new ArrayList<>();
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));

                binaryVariableValues.add(new BytesRef(randomAlphaOfLength(between(0, 100))));
                d.add(new BinaryDocValuesField(binaryFixedField, new BytesRef(randomAlphaOfLength(binaryFixedLength))));
                d.add(new BinaryDocValuesField(binaryVariableField, binaryVariableValues.getLast()));

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000L;
                }
            }
            iw.commit();

            var factory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                for (var leaf : reader.leaves()) {
                    var binaryFixedDV = getTSDBBinaryValues(leaf.reader(), binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leaf.reader(), binaryVariableField);

                    int maxDoc = leaf.reader().maxDoc();
                    for (int i = 0; i < maxDoc;) {
                        int size = Math.max(1, random().nextInt(0, maxDoc - i));
                        var docs = TestBlock.docs(IntStream.range(i, i + size).toArray());
                        {
                            // bulk loading binary fixed length field:
                            var block = (TestBlock) binaryFixedDV.tryReadLength(factory, docs, 0, random().nextBoolean());
                            assertNotNull(block);
                            assertEquals(size, block.size());

                            for (int j = 0; j < block.size(); j++) {
                                var actual = (int) block.get(j);
                                assertEquals(binaryFixedLength, actual);
                            }
                        }
                        {
                            // bulk loading binary variable length field:
                            var block = (TestBlock) binaryVariableDV.tryReadLength(factory, docs, 0, random().nextBoolean());
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                var actual = (int) block.get(j);
                                var expected = binaryVariableValues.removeLast().length;
                                assertEquals(expected, actual);
                            }
                        }

                        i += size;
                    }
                }
            }

            // Now bulk reader from one big segment and use random offset:
            iw.forceMerge(1);
            try (var reader = DirectoryReader.open(iw)) {
                int randomOffset = random().nextInt(numDocs / 4);
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();
                int maxDoc = leafReader.maxDoc();
                int size = maxDoc - randomOffset;

                {
                    var binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);
                    var docs = TestBlock.docs(IntStream.range(0, maxDoc).toArray());

                    // Separate doc values to bulk load and use as expected values
                    var expectedBinaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var expectedBinaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Test fixed length
                    var fixedBinaryBlock = (TestBlock) binaryFixedDV.tryReadLength(factory, docs, randomOffset, false);
                    assertLengthDenseBlock(fixedBinaryBlock, size, randomOffset, docs, expectedBinaryFixedDV);

                    // Test variable length
                    var variableBinaryBlock = (TestBlock) binaryVariableDV.tryReadLength(factory, docs, randomOffset, false);
                    assertLengthDenseBlock(variableBinaryBlock, size, randomOffset, docs, expectedBinaryVariableDV);
                }

                {
                    // And finally docs with gaps:
                    var docs = TestBlock.docs(IntStream.range(0, maxDoc).filter(docId -> docId == 0 || docId % 64 != 0).toArray());
                    size = docs.count();

                    // Doc values to call tryReadLength on:
                    var binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Doc values to get expected lengths from
                    var expectedBinaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var expectedBinaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Test fixed length
                    var fixedBinaryBlock = (TestBlock) binaryFixedDV.tryReadLength(factory, docs, 0, false);
                    assertLengthDenseBlock(fixedBinaryBlock, size, 0, docs, expectedBinaryFixedDV);

                    // Test variable length
                    var variableBinaryBlock = (TestBlock) binaryVariableDV.tryReadLength(factory, docs, 0, false);
                    assertLengthDenseBlock(variableBinaryBlock, size, 0, docs, expectedBinaryVariableDV);
                }
            }
        }
    }

    public void testOptionalLengthReaderSparse() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFixedLength = randomIntBetween(0, 100);
        long currentTimestamp = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            List<BytesRef> binaryVariableValues = new ArrayList<>();
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));

                if (randomBoolean()) {
                    binaryVariableValues.add(new BytesRef(randomAlphaOfLength(between(0, 100))));
                    d.add(new BinaryDocValuesField(binaryFixedField, new BytesRef(randomAlphaOfLength(binaryFixedLength))));
                    d.add(new BinaryDocValuesField(binaryVariableField, binaryVariableValues.getLast()));
                } else {
                    binaryVariableValues.add(null);
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000L;
                }
            }
            iw.commit();

            // Now bulk reader from one big segment and use random offset:
            var factory = TestBlock.factory();
            iw.forceMerge(1);
            try (var reader = DirectoryReader.open(iw)) {
                int randomOffset = random().nextInt(numDocs / 4);
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();
                int maxDoc = leafReader.maxDoc();
                int size = maxDoc - randomOffset;

                {
                    var binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);
                    var docs = TestBlock.docs(IntStream.range(0, maxDoc).toArray());

                    // Separate doc values to bulk load and use as expected values
                    var expectedBinaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var expectedBinaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Test fixed length
                    var fixedBinaryBlock = (TestBlock) binaryFixedDV.tryReadLength(factory, docs, randomOffset, false);
                    assertLengthSparseBlock(fixedBinaryBlock, size, randomOffset, docs, expectedBinaryFixedDV);

                    // Test variable length
                    var variableBinaryBlock = (TestBlock) binaryVariableDV.tryReadLength(factory, docs, randomOffset, false);
                    assertLengthSparseBlock(variableBinaryBlock, size, randomOffset, docs, expectedBinaryVariableDV);
                }

                {
                    // And finally docs with gaps:
                    var docs = TestBlock.docs(IntStream.range(0, maxDoc).filter(docId -> docId == 0 || docId % 64 != 0).toArray());
                    size = docs.count();

                    // Doc values to call tryReadLength on:
                    var binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Doc values to get expected lengths from
                    var expectedBinaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                    var expectedBinaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                    // Test fixed length
                    var fixedBinaryBlock = (TestBlock) binaryFixedDV.tryReadLength(factory, docs, 0, false);
                    assertLengthSparseBlock(fixedBinaryBlock, size, 0, docs, expectedBinaryFixedDV);

                    // Test variable length
                    var variableBinaryBlock = (TestBlock) binaryVariableDV.tryReadLength(factory, docs, 0, false);
                    assertLengthSparseBlock(variableBinaryBlock, size, 0, docs, expectedBinaryVariableDV);
                }
            }
        }
    }

    private void assertLengthSparseBlock(
        TestBlock block,
        int size,
        int randomOffset,
        BlockLoader.Docs docs,
        BinaryDocValues expectedBytesRefs
    ) throws IOException {
        assertNotNull(block);
        assertEquals(size, block.size());
        for (int j = 0; j < block.size(); j++) {
            var actual = block.get(j);
            if (expectedBytesRefs.advanceExact(docs.get(randomOffset + j))) {
                var expected = expectedBytesRefs.binaryValue().length;
                assertEquals(expected, actual);
            } else {
                assertNull(actual);
            }
        }
    }

    private void assertLengthDenseBlock(
        TestBlock block,
        int size,
        int randomOffset,
        BlockLoader.Docs docs,
        BinaryDocValues expectedBytesRefs
    ) throws IOException {
        assertNotNull(block);
        assertEquals(size, block.size());
        for (int j = 0; j < block.size(); j++) {
            var actual = block.get(j);
            assert (expectedBytesRefs.advanceExact(docs.get(randomOffset + j)));
            var expected = expectedBytesRefs.binaryValue().length;
            assertEquals(expected, actual);
        }
    }

    public void testOptionalColumnAtATimeReader() throws Exception {
        final String counterField = "counter";
        final String counterFieldAsString = "counter_as_string";
        final String timestampField = TIMESTAMP_FIELD;
        final String gaugeField = "gauge";
        final boolean useCustomBinaryFormat = randomBoolean();
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFieldMaxLength = randomIntBetween(1, 20);
        long currentTimestamp = BASE_TIMESTAMP;
        long currentCounter = 10_000_000;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long[] gauge1Values = new long[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            List<BytesRef> binaryFixedValues = new ArrayList<>();
            Set<BytesRef> uniqueBinaryFixedValues = new HashSet<>();
            List<BytesRef> binaryVariableValues = new ArrayList<>();
            Set<BytesRef> uniqueBinaryVariableValues = new HashSet<>();
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                binaryFixedValues.add(new BytesRef(randomAlphaOfLength(binaryFieldMaxLength)));
                uniqueBinaryFixedValues.add(binaryFixedValues.getLast());
                binaryVariableValues.add(new BytesRef(randomAlphaOfLength(between(0, binaryFieldMaxLength))));
                uniqueBinaryVariableValues.add(binaryVariableValues.getLast());
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));
                d.add(new SortedNumericDocValuesField(counterField, currentCounter));
                d.add(new SortedSetDocValuesField(counterFieldAsString, new BytesRef(Long.toString(currentCounter))));
                d.add(new SortedNumericDocValuesField(gaugeField, gauge1Values[i % gauge1Values.length]));
                if (useCustomBinaryFormat) {
                    byte[] bytes = binaryFixedValues.getLast().utf8ToString().getBytes(StandardCharsets.UTF_8);
                    d.add(new CustomBinaryDocValuesField(binaryFixedField, bytes));
                } else {
                    d.add(new BinaryDocValuesField(binaryFixedField, binaryFixedValues.getLast()));
                }
                if (useCustomBinaryFormat) {
                    byte[] bytes = binaryVariableValues.getLast().utf8ToString().getBytes(StandardCharsets.UTF_8);
                    d.add(new CustomBinaryDocValuesField(binaryVariableField, bytes));
                } else {
                    d.add(new BinaryDocValuesField(binaryVariableField, binaryVariableValues.getLast()));
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000L;
                    currentCounter++;
                }
            }
            iw.commit();
            var factory = TestBlock.factory();
            final long lastIndexedTimestamp = currentTimestamp;
            final long lastIndexedCounter = currentCounter;
            try (var reader = DirectoryReader.open(iw)) {
                int gaugeIndex = numDocs;
                for (var leaf : reader.leaves()) {
                    var timestampDV = getBaseDenseNumericValues(leaf.reader(), timestampField);
                    var counterDV = getBaseDenseNumericValues(leaf.reader(), counterField);
                    var gaugeDV = getBaseDenseNumericValues(leaf.reader(), gaugeField);
                    var stringCounterDV = getBaseSortedDocValues(leaf.reader(), counterFieldAsString);
                    var binaryFixedDV = getTSDBBinaryValues(leaf.reader(), binaryFixedField);
                    var binaryVariableDV = getTSDBBinaryValues(leaf.reader(), binaryVariableField);

                    int maxDoc = leaf.reader().maxDoc();
                    for (int i = 0; i < maxDoc;) {
                        int size = Math.max(1, random().nextInt(0, maxDoc - i));
                        var docs = TestBlock.docs(IntStream.range(i, i + size).toArray());
                        int docIdEnd = docs.get(docs.count() - 1);

                        {
                            // bulk loading timestamp:
                            var block = (TestBlock) timestampDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                            assertEquals(timestampDV.docID(), docIdEnd);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                long actualTimestamp = (long) block.get(j);
                                long expectedTimestamp = currentTimestamp;
                                assertEquals(expectedTimestamp, actualTimestamp);
                                currentTimestamp -= 1000L;
                            }
                        }
                        {
                            // bulk loading counter field:
                            var block = (TestBlock) counterDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                            assertEquals(counterDV.docID(), docIdEnd);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            var stringBlock = (TestBlock) stringCounterDV.tryRead(
                                factory,
                                docs,
                                0,
                                random().nextBoolean(),
                                null,
                                false,
                                false
                            );
                            assertEquals(stringCounterDV.docID(), docIdEnd);
                            assertNotNull(stringBlock);
                            assertEquals(size, stringBlock.size());
                            for (int j = 0; j < block.size(); j++) {
                                long expectedCounter = currentCounter;
                                long actualCounter = (long) block.get(j);
                                assertEquals(expectedCounter, actualCounter);

                                var expectedStringCounter = Long.toString(actualCounter);
                                var actualStringCounter = ((BytesRef) stringBlock.get(j)).utf8ToString();
                                assertEquals(expectedStringCounter, actualStringCounter);

                                currentCounter--;
                            }
                        }
                        {
                            // bulk loading gauge field:
                            var block = (TestBlock) gaugeDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                            assertEquals(gaugeDV.docID(), docIdEnd);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                long actualGauge = (long) block.get(j);
                                long expectedGauge = gauge1Values[--gaugeIndex % gauge1Values.length];
                                assertEquals(expectedGauge, actualGauge);
                            }
                        }
                        {
                            // bulk loading binary fixed length field:
                            var block = (TestBlock) binaryFixedDV.tryRead(
                                factory,
                                docs,
                                0,
                                random().nextBoolean(),
                                null,
                                false,
                                useCustomBinaryFormat
                            );
                            assertEquals(binaryFixedDV.docID(), docIdEnd);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                var actual = (BytesRef) block.get(j);
                                var expected = binaryFixedValues.removeLast();
                                assertEquals(expected, actual);
                            }
                        }
                        {
                            // bulk loading binary variable length field:
                            var block = (TestBlock) binaryVariableDV.tryRead(
                                factory,
                                docs,
                                0,
                                random().nextBoolean(),
                                null,
                                false,
                                useCustomBinaryFormat
                            );
                            assertEquals(binaryVariableDV.docID(), docIdEnd);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                var actual = (BytesRef) block.get(j);
                                var expected = binaryVariableValues.removeLast();
                                assertEquals(expected, actual);
                            }
                        }

                        i += size;
                    }
                }
            }

            // Now bulk reader from one big segment and use random offset:
            iw.forceMerge(1);
            var blockFactory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                int randomOffset = random().nextInt(numDocs / 4);
                currentTimestamp = lastIndexedTimestamp - (randomOffset * 1000L);
                currentCounter = lastIndexedCounter - randomOffset;
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();
                int maxDoc = leafReader.maxDoc();
                int size = maxDoc - randomOffset;
                int gaugeIndex = size;

                var timestampDV = getBaseDenseNumericValues(leafReader, timestampField);
                var counterDV = getBaseDenseNumericValues(leafReader, counterField);
                var gaugeDV = getBaseDenseNumericValues(leafReader, gaugeField);
                var stringCounterDV = getBaseSortedDocValues(leafReader, counterFieldAsString);
                var binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                var binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);

                var docs = TestBlock.docs(IntStream.range(0, maxDoc).toArray());
                int docIdEnd = docs.get(docs.count() - 1);
                {
                    // bulk loading timestamp:
                    var block = (TestBlock) timestampDV.tryRead(blockFactory, docs, randomOffset, false, null, false, false);
                    assertEquals(timestampDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());
                    for (int j = 0; j < block.size(); j++) {
                        long actualTimestamp = (long) block.get(j);
                        long expectedTimestamp = currentTimestamp;
                        assertEquals(expectedTimestamp, actualTimestamp);
                        currentTimestamp -= 1000L;
                    }
                }
                {
                    // bulk loading counter field:
                    var block = (TestBlock) counterDV.tryRead(factory, docs, randomOffset, false, null, false, false);
                    assertEquals(counterDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());

                    var stringBlock = (TestBlock) stringCounterDV.tryRead(factory, docs, randomOffset, false, null, false, false);
                    assertEquals(stringCounterDV.docID(), docIdEnd);
                    assertNotNull(stringBlock);
                    assertEquals(size, stringBlock.size());

                    for (int j = 0; j < block.size(); j++) {
                        long actualCounter = (long) block.get(j);
                        long expectedCounter = currentCounter;
                        assertEquals(expectedCounter, actualCounter);

                        var expectedStringCounter = Long.toString(actualCounter);
                        var actualStringCounter = ((BytesRef) stringBlock.get(j)).utf8ToString();
                        assertEquals(expectedStringCounter, actualStringCounter);

                        currentCounter--;
                    }
                }
                {
                    // bulk loading gauge field:
                    var block = (TestBlock) gaugeDV.tryRead(factory, docs, randomOffset, false, null, false, false);
                    assertEquals(gaugeDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());
                    for (int j = 0; j < block.size(); j++) {
                        long actualGauge = (long) block.get(j);
                        long expectedGauge = gauge1Values[--gaugeIndex % gauge1Values.length];
                        assertEquals(expectedGauge, actualGauge);
                    }
                }
                {
                    // bulk loading binary fixed length field:
                    var block = (TestBlock) binaryFixedDV.tryRead(factory, docs, randomOffset, false, null, false, useCustomBinaryFormat);
                    assertEquals(binaryFixedDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());
                    for (int j = 0; j < block.size(); j++) {
                        var actual = (BytesRef) block.get(j);
                        assertTrue("unexpected value [" + actual.utf8ToString() + "]", uniqueBinaryFixedValues.contains(actual));
                    }
                }
                {
                    // bulk loading binary variable length field:
                    var block = (TestBlock) binaryVariableDV.tryRead(
                        factory,
                        docs,
                        randomOffset,
                        false,
                        null,
                        false,
                        useCustomBinaryFormat
                    );
                    assertEquals(binaryVariableDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());
                    for (int j = 0; j < block.size(); j++) {
                        var actual = (BytesRef) block.get(j);
                        assertTrue("unexpected value [" + actual.utf8ToString() + "]", uniqueBinaryVariableValues.contains(actual));
                    }
                }

                // And finally docs with gaps:
                docs = TestBlock.docs(IntStream.range(0, maxDoc).filter(docId -> docId == 0 || docId % 64 != 0).toArray());
                docIdEnd = docs.get(docs.count() - 1);
                size = docs.count();
                // Test against values loaded using normal doc value apis:
                long[] expectedCounters = new long[size];
                counterDV = getBaseDenseNumericValues(leafReader, counterField);
                List<BytesRef> expectedFixedBinaryValues = new ArrayList<>();
                binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                List<BytesRef> expectedVariableBinaryValues = new ArrayList<>();
                binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);
                final var cdvReader = new CustomBinaryDocValuesReader();
                for (int i = 0; i < docs.count(); i++) {
                    int docId = docs.get(i);
                    counterDV.advanceExact(docId);
                    expectedCounters[i] = counterDV.longValue();
                    binaryFixedDV.advanceExact(docId);
                    if (useCustomBinaryFormat) {
                        cdvReader.read(binaryFixedDV.binaryValue(), new BytesRefBuilderStub() {
                            @Override
                            public BlockLoader.BytesRefBuilder appendBytesRef(BytesRef value) {
                                expectedFixedBinaryValues.add(BytesRef.deepCopyOf(value));
                                return this;
                            }
                        });
                        binaryVariableDV.advanceExact(docId);
                        cdvReader.read(binaryVariableDV.binaryValue(), new BytesRefBuilderStub() {
                            @Override
                            public BlockLoader.BytesRefBuilder appendBytesRef(BytesRef value) {
                                expectedVariableBinaryValues.add(BytesRef.deepCopyOf(value));
                                return this;
                            }
                        });
                    } else {
                        expectedFixedBinaryValues.add(BytesRef.deepCopyOf(binaryFixedDV.binaryValue()));
                        binaryVariableDV.advanceExact(docId);
                        expectedVariableBinaryValues.add(BytesRef.deepCopyOf(binaryVariableDV.binaryValue()));
                    }

                }
                counterDV = getBaseDenseNumericValues(leafReader, counterField);
                stringCounterDV = getBaseSortedDocValues(leafReader, counterFieldAsString);
                binaryFixedDV = getTSDBBinaryValues(leafReader, binaryFixedField);
                binaryVariableDV = getTSDBBinaryValues(leafReader, binaryVariableField);
                {
                    // bulk loading counter field:
                    var block = (TestBlock) counterDV.tryRead(factory, docs, 0, false, null, false, false);
                    assertEquals(counterDV.docID(), docIdEnd);
                    assertNotNull(block);
                    assertEquals(size, block.size());

                    var stringBlock = (TestBlock) stringCounterDV.tryRead(factory, docs, 0, false, null, false, false);
                    assertEquals(stringCounterDV.docID(), docIdEnd);
                    assertNotNull(stringBlock);
                    assertEquals(size, stringBlock.size());

                    var fixedBinaryBlock = (TestBlock) binaryFixedDV.tryRead(factory, docs, 0, false, null, false, useCustomBinaryFormat);
                    assertEquals(binaryFixedDV.docID(), docIdEnd);
                    assertNotNull(fixedBinaryBlock);
                    assertEquals(size, fixedBinaryBlock.size());

                    var variableBinaryBlock = (TestBlock) binaryVariableDV.tryRead(
                        factory,
                        docs,
                        0,
                        false,
                        null,
                        false,
                        useCustomBinaryFormat
                    );
                    assertEquals(binaryVariableDV.docID(), docIdEnd);
                    assertNotNull(variableBinaryBlock);
                    assertEquals(size, variableBinaryBlock.size());

                    for (int j = 0; j < block.size(); j++) {
                        long actualCounter = (long) block.get(j);
                        long expectedCounter = expectedCounters[j];
                        assertEquals(expectedCounter, actualCounter);

                        var expectedStringCounter = Long.toString(actualCounter);
                        var actualStringCounter = ((BytesRef) stringBlock.get(j)).utf8ToString();
                        assertEquals(expectedStringCounter, actualStringCounter);

                        var expectedFixedBinary = expectedFixedBinaryValues.get(j);
                        var actualFixedBinary = (BytesRef) fixedBinaryBlock.get(j);
                        assertEquals(expectedFixedBinary, actualFixedBinary);

                        var expectedVariableBinary = expectedVariableBinaryValues.get(j);
                        var actualVariableBinary = (BytesRef) variableBinaryBlock.get(j);
                        assertEquals(expectedVariableBinary, actualVariableBinary);
                    }
                }
            }
        }
    }

    public void testOptionalColumnAtATimeReaderReadAsInt() throws Exception {
        final String counterField = "counter";
        final String timestampField = TIMESTAMP_FIELD;
        final String gaugeField = "gauge";
        int currentTimestamp = 17040672;
        int currentCounter = 10_000_000;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int[] gauge1Values = new int[] { 2, 4, 6, 8, 10, 12, 14, 16 };
            int numDocs = 256 + random().nextInt(8096);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                long timestamp = currentTimestamp;
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, timestamp));
                d.add(new SortedNumericDocValuesField(counterField, currentCounter));
                d.add(new SortedNumericDocValuesField(gaugeField, gauge1Values[i % gauge1Values.length]));

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentTimestamp += 1000;
                    currentCounter++;
                }
            }
            iw.commit();
            var factory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                int gaugeIndex = numDocs;
                for (var leaf : reader.leaves()) {
                    var timestampDV = getBaseDenseNumericValues(leaf.reader(), timestampField);
                    var counterDV = getBaseDenseNumericValues(leaf.reader(), counterField);
                    var gaugeDV = getBaseDenseNumericValues(leaf.reader(), gaugeField);
                    int maxDoc = leaf.reader().maxDoc();
                    for (int i = 0; i < maxDoc;) {
                        int size = Math.max(1, random().nextInt(0, maxDoc - i));
                        var docs = TestBlock.docs(IntStream.range(i, i + size).toArray());

                        {
                            // bulk loading timestamp:
                            var block = (TestBlock) timestampDV.tryRead(factory, docs, 0, random().nextBoolean(), null, true, false);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                int actualTimestamp = (int) block.get(j);
                                int expectedTimestamp = currentTimestamp;
                                assertEquals(expectedTimestamp, actualTimestamp);
                                currentTimestamp -= 1000;
                            }
                        }
                        {
                            // bulk loading counter field:
                            var block = (TestBlock) counterDV.tryRead(factory, docs, 0, random().nextBoolean(), null, true, false);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                int expectedCounter = currentCounter;
                                int actualCounter = (int) block.get(j);
                                assertEquals(expectedCounter, actualCounter);
                                currentCounter--;
                            }
                        }
                        {
                            // bulk loading gauge field:
                            var block = (TestBlock) gaugeDV.tryRead(factory, docs, 0, random().nextBoolean(), null, true, false);
                            assertNotNull(block);
                            assertEquals(size, block.size());
                            for (int j = 0; j < block.size(); j++) {
                                int actualGauge = (int) block.get(j);
                                int expectedGauge = gauge1Values[--gaugeIndex % gauge1Values.length];
                                assertEquals(expectedGauge, actualGauge);
                            }
                        }

                        i += size;
                    }
                }
            }
        }
    }

    public void testOptionalColumnAtATimeReaderBinary() throws Exception {
        final boolean useCustomBinaryFormat = randomBoolean();
        final String binaryFieldOne = "binary_1";
        final String binaryFieldTwo = "binary_2";

        var config = new IndexWriterConfig();
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(getCodec());

        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            Set<String> binaryValues = new HashSet<>();
            int numDocs = 10_000 * randomIntBetween(2, 20);

            int numValues = randomIntBetween(8, 256);
            for (int i = 0; i < numValues; i++) {
                binaryValues.add(randomAlphaOfLength(between(128, 256)));
            }

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                if (useCustomBinaryFormat) {
                    d.add(new CustomBinaryDocValuesField(binaryFieldOne, randomFrom(binaryValues).getBytes(StandardCharsets.UTF_8)));
                } else {
                    d.add(new BinaryDocValuesField(binaryFieldOne, new BytesRef(randomFrom(binaryValues))));
                }

                int valuesPerDoc = randomIntBetween(2, 8);
                Set<String> values = new HashSet<>();
                while (values.size() < valuesPerDoc) {
                    values.add(randomFrom(binaryValues));
                }
                CustomBinaryDocValuesField fieldTwo = null;
                for (String value : values) {
                    if (fieldTwo == null) {
                        fieldTwo = new CustomBinaryDocValuesField(binaryFieldTwo, value.getBytes(StandardCharsets.UTF_8));
                    } else {
                        fieldTwo.add(value.getBytes(StandardCharsets.UTF_8));
                    }
                }
                d.add(fieldTwo);

                iw.addDocument(d);
                if (i % 1000 == 0) {
                    iw.commit();
                }
            }
            iw.commit();
            var factory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                for (var leaf : reader.leaves()) {
                    int maxDoc = leaf.reader().maxDoc();
                    var binaryDVField1 = getTSDBBinaryValues(leaf.reader(), binaryFieldOne);
                    // Randomize start doc, starting from a docid that is part of later blocks triggers:
                    // https://github.com/elastic/elasticsearch/issues/138750
                    var docs = TestBlock.docs(IntStream.range(between(0, maxDoc - 1), maxDoc).toArray());
                    var block = (TestBlock) binaryDVField1.tryRead(
                        factory,
                        docs,
                        0,
                        random().nextBoolean(),
                        null,
                        false,
                        useCustomBinaryFormat
                    );
                    assertNotNull(block);
                    assertTrue(block.size() > 0);
                    for (int j = 0; j < block.size(); j++) {
                        var actual = ((BytesRef) block.get(j)).utf8ToString();
                        assertTrue("actual [" + actual + "] not in generated values", binaryValues.contains(actual));
                    }

                    var binaryDVField2 = getTSDBBinaryValues(leaf.reader(), binaryFieldTwo);
                    block = (TestBlock) binaryDVField2.tryRead(factory, docs, 0, random().nextBoolean(), null, false, true);
                    for (int j = 0; j < Objects.requireNonNull(block).size(); j++) {
                        var values = (List<?>) block.get(j);
                        assertFalse(values.isEmpty());
                        for (Object value : values) {
                            var actual = ((BytesRef) value).utf8ToString();
                            assertTrue("actual [" + actual + "] not in generated values", binaryValues.contains(actual));
                        }
                    }
                }
            }
        }
    }

    public void testOptionalColumnAtATimeReaderWithSparseDocs() throws Exception {
        final String counterField = "counter";
        final String counterAsStringField = "counter_as_string";
        final String timestampField = TIMESTAMP_FIELD;
        String queryField = "query_field";
        String temperatureField = "temperature_field";
        final String binaryFixedField = "binary_variable";
        final String binaryVariableField = "binary_fixed";
        final int binaryFieldMaxLength = randomIntBetween(1, 20);
        boolean denseBinaryData = randomBoolean();

        long currentTimestamp = BASE_TIMESTAMP;
        long currentCounter = 10_000_000;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int numDocsPerQValue = 120;
            int numDocs = numDocsPerQValue * (1 + random().nextInt(40));
            Long[] temperatureValues = new Long[numDocs];
            BytesRef[] binaryFixed = new BytesRef[numDocs];
            BytesRef[] binaryVariable = new BytesRef[numDocs];
            long q = 1;
            for (int i = 1; i <= numDocs; i++) {
                var d = new Document();
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(SortedNumericDocValuesField.indexedField(timestampField, currentTimestamp));
                currentTimestamp += 1000L;
                d.add(new SortedNumericDocValuesField(counterField, currentCounter));
                d.add(new SortedDocValuesField(counterAsStringField, new BytesRef(Long.toString(currentCounter))));
                d.add(new SortedNumericDocValuesField(queryField, q));

                if (denseBinaryData || random().nextBoolean()) {
                    binaryFixed[numDocs - i] = new BytesRef(randomAlphaOfLength(binaryFieldMaxLength));
                    d.add(new BinaryDocValuesField(binaryFixedField, binaryFixed[numDocs - i]));
                    binaryVariable[numDocs - i] = new BytesRef(randomAlphaOfLength(between(0, binaryFieldMaxLength)));
                    d.add(new BinaryDocValuesField(binaryVariableField, binaryVariable[numDocs - i]));
                }

                if (i % 120 == 0) {
                    q++;
                }
                if (random().nextBoolean()) {
                    long v = random().nextLong();
                    temperatureValues[numDocs - i] = v;
                    d.add(new NumericDocValuesField(temperatureField, v));
                }
                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                if (i < numDocs - 1) {
                    currentCounter++;
                }
            }
            iw.commit();

            // Now bulk reader from one big segment and use random offset:
            iw.forceMerge(1);
            var factory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();

                for (int query = 1; query < q; query++) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    var topDocs = searcher.search(
                        SortedNumericDocValuesField.newSlowExactQuery(queryField, query),
                        numDocsPerQValue,
                        new Sort(SortField.FIELD_DOC),
                        false
                    );
                    assertEquals(numDocsPerQValue, topDocs.totalHits.value());
                    var timestampDV = getBaseDenseNumericValues(leafReader, timestampField);
                    long[] expectedTimestamps = new long[numDocsPerQValue];
                    var counterDV = getBaseDenseNumericValues(leafReader, counterField);
                    long[] expectedCounters = new long[numDocsPerQValue];
                    var counterAsStringDV = getBaseSortedDocValues(leafReader, counterAsStringField);
                    String[] expectedCounterAsStrings = new String[numDocsPerQValue];

                    int[] docIds = new int[numDocsPerQValue];
                    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                        var scoreDoc = topDocs.scoreDocs[i];
                        docIds[i] = scoreDoc.doc;

                        assertTrue(timestampDV.advanceExact(scoreDoc.doc));
                        expectedTimestamps[i] = timestampDV.longValue();

                        assertTrue(counterDV.advanceExact(scoreDoc.doc));
                        expectedCounters[i] = counterDV.longValue();

                        assertTrue(counterAsStringDV.advanceExact(scoreDoc.doc));
                        expectedCounterAsStrings[i] = counterAsStringDV.lookupOrd(counterAsStringDV.ordValue()).utf8ToString();
                    }

                    var docs = TestBlock.docs(docIds);
                    {
                        timestampDV = getBaseDenseNumericValues(leafReader, timestampField);
                        var block = (TestBlock) timestampDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                        assertNotNull(block);
                        assertEquals(numDocsPerQValue, block.size());
                        for (int j = 0; j < block.size(); j++) {
                            long actualTimestamp = (long) block.get(j);
                            long expectedTimestamp = expectedTimestamps[j];
                            assertEquals(expectedTimestamp, actualTimestamp);
                        }
                    }
                    {
                        counterDV = getBaseDenseNumericValues(leafReader, counterField);
                        var block = (TestBlock) counterDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                        assertNotNull(block);
                        assertEquals(numDocsPerQValue, block.size());
                        for (int j = 0; j < block.size(); j++) {
                            long actualCounter = (long) block.get(j);
                            long expectedCounter = expectedCounters[j];
                            assertEquals(expectedCounter, actualCounter);
                        }
                    }
                    {
                        counterAsStringDV = getBaseSortedDocValues(leafReader, counterAsStringField);
                        var block = (TestBlock) counterAsStringDV.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                        assertNotNull(block);
                        assertEquals(numDocsPerQValue, block.size());
                        for (int j = 0; j < block.size(); j++) {
                            var actualCounter = ((BytesRef) block.get(j)).utf8ToString();
                            var expectedCounter = expectedCounterAsStrings[j];
                            assertEquals(expectedCounter, actualCounter);
                        }
                    }
                }

                BlockLoader.Docs docs;
                {
                    int startIndex = ESTestCase.between(0, temperatureValues.length - 1);
                    int endIndex = ESTestCase.between(startIndex + 1, temperatureValues.length);
                    List<Integer> testDocs = new ArrayList<>();
                    for (int i = startIndex; i < endIndex; i++) {
                        if (temperatureValues[i] != null) {
                            testDocs.add(i);
                        }
                    }
                    if (testDocs.isEmpty() == false) {
                        NumericDocValues dv = leafReader.getNumericDocValues(temperatureField);
                        assertThat(dv, instanceOf(OptionalColumnAtATimeReader.class));
                        OptionalColumnAtATimeReader directReader = (OptionalColumnAtATimeReader) dv;
                        docs = TestBlock.docs(testDocs.stream().mapToInt(n -> n).toArray());
                        assertNull(directReader.tryRead(factory, docs, 0, false, null, false, false));
                        TestBlock block = (TestBlock) directReader.tryRead(factory, docs, 0, true, null, false, false);
                        assertNotNull(block);
                        for (int i = 0; i < testDocs.size(); i++) {
                            assertThat(block.get(i), equalTo(temperatureValues[testDocs.get(i)]));
                        }
                    }
                    if (testDocs.size() > 2) {
                        // currently bulk loading is disabled with gaps
                        testDocs.remove(ESTestCase.between(1, testDocs.size() - 2));
                        docs = TestBlock.docs(testDocs.stream().mapToInt(n -> n).toArray());
                        NumericDocValues dv = leafReader.getNumericDocValues(temperatureField);
                        OptionalColumnAtATimeReader directReader = (OptionalColumnAtATimeReader) dv;
                        assertNull(directReader.tryRead(factory, docs, 0, false, null, false, false));
                        assertNull(directReader.tryRead(factory, docs, 0, true, null, false, false));
                    }
                }

                {
                    // Bulk binary loader can only handle sparse queries over dense or sparse documents
                    List<Integer> testDocs = IntStream.range(0, numDocs - 1).filter(i -> randomBoolean()).boxed().toList();
                    docs = TestBlock.docs(testDocs.stream().mapToInt(n -> n).toArray());
                    if (testDocs.isEmpty() == false) {
                        if (denseBinaryData) {
                            {
                                var dv = getTSDBBinaryValues(leafReader, binaryFixedField);
                                var block = (TestBlock) dv.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                                assertNotNull(block);
                                for (int i = 0; i < testDocs.size(); i++) {
                                    assertThat(block.get(i), equalTo(binaryFixed[testDocs.get(i)]));
                                }
                            }
                            {
                                var dv = getTSDBBinaryValues(leafReader, binaryVariableField);
                                var block = (TestBlock) dv.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                                assertNotNull(block);
                                for (int i = 0; i < testDocs.size(); i++) {
                                    assertThat(block.get(i), equalTo(binaryVariable[testDocs.get(i)]));
                                }
                            }
                        } else {
                            {
                                var dv = getTSDBBinaryValues(leafReader, binaryFixedField);
                                var block = (TestBlock) dv.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                                assertNull(block);
                            }
                            {
                                var dv = getTSDBBinaryValues(leafReader, binaryVariableField);
                                var block = (TestBlock) dv.tryRead(factory, docs, 0, random().nextBoolean(), null, false, false);
                                assertNull(block);
                            }
                        }
                    }
                }
            }
        }
    }

    public void testEncodeRangeWithSortedSetPrimarySortField() throws Exception {
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;
        long lowerBound = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(hostnameField, true, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {

            int numDocs = 512 + random().nextInt(512);
            int numHosts = numDocs / 20;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                int batchIndex = i / numHosts;
                {
                    String hostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                    d.add(new SortedSetDocValuesField(hostnameField, new BytesRef(hostName)));
                }
                {
                    String hostName = String.format(Locale.ROOT, "host-%03d", batchIndex + 1);
                    d.add(new SortedSetDocValuesField(hostnameField, new BytesRef(hostName)));
                }
                // Index sorting doesn't work with NumericDocValuesField:
                long timestamp = lowerBound + (1000L * i);
                d.add(new SortedNumericDocValuesField(timestampField, timestamp));
                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().getFirst().reader();
                var hostNameDV = leaf.getSortedSetDocValues(hostnameField);
                assertNotNull(hostNameDV);
                var timestampDV = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(timestampField));
                assertNotNull(timestampDV);
                for (int i = 0; i < numDocs; i++) {
                    assertEquals(i, hostNameDV.nextDoc());

                    int batchIndex = i / numHosts;
                    assertEquals(2, hostNameDV.docValueCount());

                    long firstOrd = hostNameDV.nextOrd();
                    assertEquals(batchIndex, firstOrd);
                    String expectedFirstHostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                    String actualFirstHostName = hostNameDV.lookupOrd(firstOrd).utf8ToString();
                    assertEquals(expectedFirstHostName, actualFirstHostName);

                    batchIndex++;
                    long secondOrd = hostNameDV.nextOrd();
                    assertEquals(batchIndex, secondOrd);
                    String expectedSecondHostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                    String actualSecondHostName = hostNameDV.lookupOrd(secondOrd).utf8ToString();
                    assertEquals(expectedSecondHostName, actualSecondHostName);

                    assertEquals(i, timestampDV.nextDoc());
                    long timestamp = timestampDV.longValue();
                    long upperBound = lowerBound + (1000L * numDocs);
                    assertTrue(
                        "unexpected timestamp [" + timestamp + "], expected between [" + lowerBound + "] and [" + upperBound + "]",
                        timestamp >= lowerBound && timestamp < upperBound
                    );
                }
            }
        }
    }

    public void testTryReadReturnsNullWithMayContainDuplicates() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String denseNumericField = "dense_numeric";
        final String binaryFixedField = "binary_fixed";
        final String binaryVariableField = "binary_variable";
        final String sortedField = "sorted_field";
        final int binaryFixedLength = randomIntBetween(1, 20);
        final int binaryVariableMaxLength = randomIntBetween(1, 20);
        long currentTimestamp = BASE_TIMESTAMP;

        var config = getTimeSeriesIndexWriterConfig(null, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int numDocs = 256 + random().nextInt(256);

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(SortedNumericDocValuesField.indexedField(timestampField, currentTimestamp));
                d.add(new SortedNumericDocValuesField(denseNumericField, random().nextLong()));
                d.add(new BinaryDocValuesField(binaryFixedField, new BytesRef(randomAlphaOfLength(binaryFixedLength))));
                d.add(new BinaryDocValuesField(binaryVariableField, new BytesRef(randomAlphaOfLengthBetween(0, binaryVariableMaxLength))));
                d.add(new SortedDocValuesField(sortedField, new BytesRef(randomAlphaOfLengthBetween(1, 10))));
                iw.addDocument(d);
                currentTimestamp += 1000L;
            }
            iw.commit();
            iw.forceMerge(1);

            var factory = TestBlock.factory();
            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                var leafReader = reader.leaves().getFirst().reader();
                int maxDoc = leafReader.maxDoc();
                var docs = TestBlock.docs(IntStream.range(0, maxDoc).toArray());
                var docsWithDups = TestBlock.docs(
                    IntStream.concat(IntStream.range(0, maxDoc), IntStream.of(between(0, maxDoc - 1))).sorted().toArray()
                );

                {
                    // Dense numeric
                    var dv = getBaseDenseNumericValues(leafReader, denseNumericField);
                    assertNotNull(dv.tryRead(factory, docs, 0, false, null, false, false));
                    dv = getBaseDenseNumericValues(leafReader, denseNumericField);
                    assertNull(dv.tryRead(factory, docsWithDups, 0, false, null, false, false));
                }
                {
                    // Dense binary fixed-length
                    var dv = getTSDBBinaryValues(leafReader, binaryFixedField);
                    assertNotNull(dv.tryRead(factory, docs, 0, false, null, false, false));
                    dv = getTSDBBinaryValues(leafReader, binaryFixedField);
                    assertNull(dv.tryRead(factory, docsWithDups, 0, false, null, false, false));
                }
                {
                    // Dense binary variable-length
                    var dv = getTSDBBinaryValues(leafReader, binaryVariableField);
                    assertNotNull(dv.tryRead(factory, docs, 0, false, null, false, false));
                    dv = getTSDBBinaryValues(leafReader, binaryVariableField);
                    assertNull(dv.tryRead(factory, docsWithDups, 0, false, null, false, false));
                }
                {
                    // Sorted doc values
                    var dv = getBaseSortedDocValues(leafReader, sortedField);
                    assertNotNull(dv.tryRead(factory, docs, 0, false, null, false, false));
                    dv = getBaseSortedDocValues(leafReader, sortedField);
                    assertNull(dv.tryRead(factory, docsWithDups, 0, false, null, false, false));
                }
            }
        }
    }

    public void testDocIDEndRun() throws IOException {
        String timestampField = TIMESTAMP_FIELD;
        String hostnameField = HOSTNAME_FIELD;

        var config = getTimeSeriesIndexWriterConfig(hostnameField, timestampField);
        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            long counter1 = 0;

            long[] gauge2Values = new long[] { -2, -4, -6, -8, -10, -12, -14, -16 };
            String[] tags = new String[] { "tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8" };

            // IndexedDISI stores ids in blocks of 4096. To test sparse end runs, we want a mixture of
            // dense and sparse blocks, so we need the gap frequency to be larger than
            // this value, but smaller than two blocks, and to index at least three blocks
            int gap_frequency = 4500 + random().nextInt(2048);
            int numDocs = 10000 + random().nextInt(10000);
            int numHosts = numDocs / 20;

            for (int i = 0; i < numDocs; i++) {
                var d = new Document();

                int batchIndex = i / numHosts;
                String hostName = String.format(Locale.ROOT, "host-%03d", batchIndex);
                long timestamp = BASE_TIMESTAMP + (1000L * i);

                d.add(new SortedDocValuesField(hostnameField, new BytesRef(hostName)));
                // Index sorting doesn't work with NumericDocValuesField:
                d.add(new SortedNumericDocValuesField(timestampField, timestamp));
                d.add(new NumericDocValuesField("counter", counter1++));
                if (i % gap_frequency != 0) {
                    d.add(new NumericDocValuesField("sparse_counter", counter1));
                }

                int numGauge2 = 1 + random().nextInt(8);
                for (int j = 0; j < numGauge2; j++) {
                    long gauge2Value = gauge2Values[(i + j) % gauge2Values.length];
                    d.add(new SortedNumericDocValuesField("gauge", gauge2Value));
                    if (i % gap_frequency != 0) {
                        d.add(new SortedNumericDocValuesField("sparse_gauge", gauge2Value));
                    }
                }

                d.add(new SortedDocValuesField("tag", new BytesRef(randomFrom(tags))));
                if (i % gap_frequency != 0) {
                    d.add(new SortedDocValuesField("sparse_tag", new BytesRef(randomFrom(tags))));
                }

                int numTags = 1 + random().nextInt(8);
                for (int j = 0; j < numTags; j++) {
                    d.add(new SortedSetDocValuesField("tags", new BytesRef(tags[(i + j) % tags.length])));
                    if (i % gap_frequency != 0) {
                        d.add(new SortedSetDocValuesField("sparse_tags", new BytesRef(tags[(i + j) % tags.length])));
                    }
                }

                d.add(new BinaryDocValuesField("tags_as_bytes", new BytesRef(tags[i % tags.length])));
                if (i % gap_frequency != 0) {
                    d.add(new BinaryDocValuesField("sparse_tags_as_bytes", new BytesRef(tags[i % tags.length])));
                }

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
            }
            iw.commit();

            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leaf = reader.leaves().getFirst().reader();
                var hostNameDV = leaf.getSortedDocValues(hostnameField);
                assertNotNull(hostNameDV);
                validateRunEnd(hostNameDV);
                var timestampDV = DocValues.unwrapSingleton(leaf.getSortedNumericDocValues(timestampField));
                assertNotNull(timestampDV);
                validateRunEnd(timestampDV);
                var counterOneDV = leaf.getNumericDocValues("counter");
                assertNotNull(counterOneDV);
                validateRunEnd(counterOneDV);
                var sparseCounter = leaf.getNumericDocValues("sparse_counter");
                assertNotNull(sparseCounter);
                validateRunEnd(sparseCounter);
                var gaugeOneDV = leaf.getSortedNumericDocValues("gauge");
                assertNotNull(gaugeOneDV);
                validateRunEnd(gaugeOneDV);
                var sparseGaugeDV = leaf.getSortedNumericDocValues("sparse_gauge");
                assertNotNull(sparseGaugeDV);
                validateRunEnd(sparseGaugeDV);
                var tagDV = leaf.getSortedDocValues("tag");
                assertNotNull(tagDV);
                validateRunEnd(tagDV);
                var sparseTagDV = leaf.getSortedDocValues("sparse_tag");
                assertNotNull(sparseTagDV);
                validateRunEnd(sparseTagDV);
                var tagsDV = leaf.getSortedSetDocValues("tags");
                assertNotNull(tagsDV);
                validateRunEnd(tagsDV);
                var sparseTagsDV = leaf.getSortedSetDocValues("sparse_tags");
                assertNotNull(sparseTagsDV);
                validateRunEnd(sparseTagsDV);
                var tagBytesDV = leaf.getBinaryDocValues("tags_as_bytes");
                assertNotNull(tagBytesDV);
                validateRunEnd(tagBytesDV);
                var sparseTagBytesDV = leaf.getBinaryDocValues("sparse_tags_as_bytes");
                assertNotNull(sparseTagBytesDV);
                validateRunEnd(sparseTagBytesDV);
            }
        }
    }

    private void validateRunEnd(DocIdSetIterator iterator) throws IOException {
        int runCount = 0;
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int runLength = iterator.docIDRunEnd() - iterator.docID() - 1;
            if (runLength > 1) {
                runCount++;
                for (int i = 0; i < runLength; i++) {
                    int expected = iterator.docID() + 1;
                    assertEquals(expected, iterator.advance(expected));
                }
            }
        }
        assertTrue("Expected docid runs of greater than 1", runCount > 0);
    }

    abstract static class BytesRefBuilderStub implements BlockLoader.BytesRefBuilder {

        @Override
        public BlockLoader.BytesRefBuilder appendBytesRef(BytesRef value) {
            return this;
        }

        @Override
        public BlockLoader.Block build() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder appendNull() {
            return this;
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            return this;
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            return this;
        }

        @Override
        public void close() {}
    }

    protected void doTestAddIndices(List<DocValuesFormat> sourceFormats) throws IOException {
        Supplier<IndexWriterConfig> indexConfigWithRandomDVFormat = () -> {
            IndexWriterConfig config = getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, TIMESTAMP_FIELD);
            DocValuesFormat dvFormat = randomFrom(sourceFormats);
            config.setCodec(new Elasticsearch900Lucene101Codec() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return dvFormat;
                }
            });
            return config;
        };
        var allNumericFields = IntStream.range(0, ESTestCase.between(1, 10)).mapToObj(n -> "numeric_" + n).toList();
        var allSortedNumericFields = IntStream.range(0, ESTestCase.between(1, 10)).mapToObj(n -> "sorted_numeric_" + n).toList();
        var allSortedFields = IntStream.range(0, ESTestCase.between(1, 10)).mapToObj(n -> "sorted_" + n).toList();
        var allSortedSetFields = IntStream.range(0, ESTestCase.between(1, 10)).mapToObj(n -> "sorted_set" + n).toList();
        var allBinaryFields = IntStream.range(0, ESTestCase.between(1, 10)).mapToObj(n -> "binary_" + n).toList();
        try (var source1 = newDirectory(); var source2 = newDirectory(); var singleDir = newDirectory(); var mergeDir = newDirectory()) {
            try (
                var writer1 = new IndexWriter(source1, indexConfigWithRandomDVFormat.get());
                var writer2 = new IndexWriter(source2, indexConfigWithRandomDVFormat.get());
                var singleWriter = new IndexWriter(singleDir, indexConfigWithRandomDVFormat.get())
            ) {
                int numDocs = 1 + random().nextInt(1_000);
                long timestamp = random().nextLong(1000_000L);
                for (int i = 0; i < numDocs; i++) {
                    List<IndexableField> fields = new ArrayList<>();
                    String hostName = String.format(Locale.ROOT, "host-%d", random().nextInt(5));
                    timestamp += 1 + random().nextInt(1_000);
                    fields.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(hostName)));
                    fields.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                    for (String f : ESTestCase.randomSubsetOf(allNumericFields)) {
                        fields.add(new NumericDocValuesField(f, random().nextLong(1000L)));
                    }
                    for (String field : ESTestCase.randomSubsetOf(allSortedNumericFields)) {
                        int valueCount = 1 + random().nextInt(3);
                        for (int v = 0; v < valueCount; v++) {
                            fields.add(new SortedNumericDocValuesField(field, random().nextLong(1000L)));
                        }
                    }
                    for (String field : ESTestCase.randomSubsetOf(allSortedFields)) {
                        fields.add(new SortedDocValuesField(field, new BytesRef("s" + random().nextInt(100))));
                    }
                    for (String field : ESTestCase.randomSubsetOf(allSortedSetFields)) {
                        int valueCount = 1 + random().nextInt(3);
                        for (int v = 0; v < valueCount; v++) {
                            fields.add(new SortedSetDocValuesField(field, new BytesRef("ss" + random().nextInt(100))));
                        }
                    }
                    for (String field : ESTestCase.randomSubsetOf(allBinaryFields)) {
                        fields.add(new BinaryDocValuesField(field, new BytesRef("b" + random().nextInt(100))));
                    }
                    for (IndexWriter writer : List.of(ESTestCase.randomFrom(writer1, writer2), singleWriter)) {
                        Randomness.shuffle(fields);
                        writer.addDocument(fields);
                        if (random().nextInt(100) <= 5) {
                            writer.commit();
                        }
                    }
                }
                if (random().nextBoolean()) {
                    writer1.forceMerge(1);
                }
                if (random().nextBoolean()) {
                    writer2.forceMerge(1);
                }
                singleWriter.commit();
                singleWriter.forceMerge(1);
            }
            try (var mergeWriter = new IndexWriter(mergeDir, getTimeSeriesIndexWriterConfig(HOSTNAME_FIELD, TIMESTAMP_FIELD))) {
                mergeWriter.addIndexes(source1, source2);
                mergeWriter.forceMerge(1);
            }
            try (var reader1 = DirectoryReader.open(singleDir); var reader2 = DirectoryReader.open(mergeDir)) {
                assertEquals(reader1.maxDoc(), reader2.maxDoc());
                assertEquals(1, reader1.leaves().size());
                assertEquals(1, reader2.leaves().size());
                for (int i = 0; i < reader1.leaves().size(); i++) {
                    LeafReader leaf1 = reader1.leaves().get(i).reader();
                    LeafReader leaf2 = reader2.leaves().get(i).reader();
                    for (String f : CollectionUtils.appendToCopy(allSortedNumericFields, TIMESTAMP_FIELD)) {
                        var dv1 = leaf1.getNumericDocValues(f);
                        var dv2 = leaf2.getNumericDocValues(f);
                        if (dv1 == null) {
                            assertNull(dv2);
                            continue;
                        }
                        assertNotNull(dv2);
                        while (dv1.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                            assertNotEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                            assertEquals(dv1.docID(), dv2.docID());
                            assertEquals(dv1.longValue(), dv2.longValue());
                        }
                        assertEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                    }
                    for (String f : CollectionUtils.appendToCopy(allSortedNumericFields, TIMESTAMP_FIELD)) {
                        var dv1 = leaf1.getSortedNumericDocValues(f);
                        var dv2 = leaf2.getSortedNumericDocValues(f);
                        if (dv1 == null) {
                            assertNull(dv2);
                            continue;
                        }
                        assertNotNull(dv2);
                        while (dv1.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                            assertNotEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                            assertEquals(dv1.docID(), dv2.docID());
                            assertEquals(dv1.docValueCount(), dv2.docValueCount());
                            for (int v = 0; v < dv1.docValueCount(); v++) {
                                assertEquals(dv1.nextValue(), dv2.nextValue());
                            }
                        }
                        assertEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                    }
                    for (String f : CollectionUtils.appendToCopy(allSortedFields, HOSTNAME_FIELD)) {
                        var dv1 = leaf1.getSortedDocValues(f);
                        var dv2 = leaf2.getSortedDocValues(f);
                        if (dv1 == null) {
                            assertNull(dv2);
                            continue;
                        }
                        assertNotNull(dv2);
                        while (dv1.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
                            assertNotEquals(SortedDocValues.NO_MORE_DOCS, dv2.nextDoc());
                            assertEquals(dv1.docID(), dv2.docID());
                            assertEquals(dv1.lookupOrd(dv1.ordValue()), dv2.lookupOrd(dv2.ordValue()));
                        }
                        assertEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                    }
                    for (String f : allSortedSetFields) {
                        var dv1 = leaf1.getSortedSetDocValues(f);
                        var dv2 = leaf2.getSortedSetDocValues(f);
                        if (dv1 == null) {
                            assertNull(dv2);
                            continue;
                        }
                        assertNotNull(dv2);
                        while (dv1.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
                            assertNotEquals(SortedDocValues.NO_MORE_DOCS, dv2.nextDoc());
                            assertEquals(dv1.docID(), dv2.docID());
                            assertEquals(dv1.docValueCount(), dv2.docValueCount());
                            for (int v = 0; v < dv1.docValueCount(); v++) {
                                assertEquals(dv1.lookupOrd(dv1.nextOrd()), dv2.lookupOrd(dv2.nextOrd()));
                            }
                        }
                        assertEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                    }
                    for (String f : allBinaryFields) {
                        var dv1 = leaf1.getBinaryDocValues(f);
                        var dv2 = leaf2.getBinaryDocValues(f);
                        if (dv1 == null) {
                            assertNull(dv2);
                            continue;
                        }
                        assertNotNull(dv2);
                        while (dv1.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
                            assertNotEquals(SortedDocValues.NO_MORE_DOCS, dv2.nextDoc());
                            assertEquals(dv1.docID(), dv2.docID());
                            assertEquals(dv1.binaryValue(), dv2.binaryValue());
                        }
                        assertEquals(NumericDocValues.NO_MORE_DOCS, dv2.nextDoc());
                    }
                }
            }
        }
    }
}
