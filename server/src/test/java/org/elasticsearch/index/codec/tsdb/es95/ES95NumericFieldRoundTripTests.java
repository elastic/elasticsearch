/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.MetricRole;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

public class ES95NumericFieldRoundTripTests extends ESTestCase {

    private static final String FIELD = "value";
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final int NUM_DOCS = 2048;

    enum FieldKind {
        NUMERIC,
        SORTED_NUMERIC
    }

    public void testSortedNumericGaugeRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, i -> i * 0.5d + 1.25d);
    }

    public void testSortedNumericCounterRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.COUNTER, i -> i * 0.5d + 1.25d);
    }

    public void testSortedNumericHistogramRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.HISTOGRAM, i -> i * 0.5d + 1.25d);
    }

    public void testSortedNumericPositionRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.POSITION, i -> i * 0.5d + 1.25d);
    }

    public void testSortedNumericWithoutMetricRoleRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, null, i -> i * 0.5d + 1.25d);
    }

    public void testSortedNumericMonotonicCounterRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.COUNTER, i -> 1_000_000.0d + i * 0.000_001d);
    }

    public void testSortedNumericNegativeRangeRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, i -> -1000.0d + i * 0.5d);
    }

    public void testSortedNumericWideMagnitudeRangeRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, i -> Math.pow(10.0d, (i % 600) - 300));
    }

    public void testSortedNumericZeroRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, i -> 0.0d);
    }

    public void testSortedNumericEdgeValuesRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, ES95NumericFieldRoundTripTests::edgeValueOrIndex);
    }

    public void testSortedNumericSortableLongRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.SORTED_NUMERIC, MetricRole.GAUGE, true, i -> Math.sin(i) * 1000.0d);
    }

    public void testNumericGaugeRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.GAUGE, i -> i * 0.5d + 1.25d);
    }

    public void testNumericCounterRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.COUNTER, i -> i * 0.5d + 1.25d);
    }

    public void testNumericHistogramRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.HISTOGRAM, i -> i * 0.5d + 1.25d);
    }

    public void testNumericPositionRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.POSITION, i -> i * 0.5d + 1.25d);
    }

    public void testNumericWithoutMetricRoleRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, null, i -> i * 0.5d + 1.25d);
    }

    public void testNumericMonotonicCounterRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.COUNTER, i -> 1_000_000.0d + i * 0.000_001d);
    }

    public void testNumericNegativeRangeRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.GAUGE, i -> -1000.0d + i * 0.5d);
    }

    public void testNumericEdgeValuesRoundTrip() throws IOException {
        assertRoundTrip(FieldKind.NUMERIC, MetricRole.GAUGE, ES95NumericFieldRoundTripTests::edgeValueOrIndex);
    }

    public void testSortedNumericMultiValuedRoundTrip() throws IOException {
        indexAndVerify(doubleResolver(name -> MetricRole.GAUGE), false, (writer, i) -> {
            final Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(FIELD, Double.doubleToLongBits(i * 0.5d)));
            if (i % 3 == 0) {
                doc.add(new SortedNumericDocValuesField(FIELD, Double.doubleToLongBits(i * 0.5d + 1.0d)));
            }
            writer.addDocument(doc);
        }, leaf -> {
            final SortedNumericDocValues values = leaf.reader().getSortedNumericDocValues(FIELD);
            assertNotNull(values);
            for (int i = 0; i < NUM_DOCS; i++) {
                assertTrue("missing doc " + i, values.advanceExact(i));
                final int expectedCount = i % 3 == 0 ? 2 : 1;
                assertEquals("doc " + i + " value count", expectedCount, values.docValueCount());
                assertEquals("doc " + i + " value 0", Double.doubleToLongBits(i * 0.5d), values.nextValue());
                if (expectedCount == 2) {
                    assertEquals("doc " + i + " value 1", Double.doubleToLongBits(i * 0.5d + 1.0d), values.nextValue());
                }
            }
        });
    }

    public void testMixedDoubleFieldsRoundTrip() throws IOException {
        final Map<String, MetricRole> roles = Map.of(
            "gauge",
            MetricRole.GAUGE,
            "counter",
            MetricRole.COUNTER,
            "histogram",
            MetricRole.HISTOGRAM,
            "position",
            MetricRole.POSITION,
            "no_role",
            MetricRole.GAUGE
        );
        indexAndVerify(doubleResolver(roles::get), true, (writer, i) -> {
            final Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, 1_700_000_000_000L + (long) i * 1_000L));
            doc.add(new SortedNumericDocValuesField("gauge", Double.doubleToLongBits(Math.sin(i) * 100.0d)));
            doc.add(new SortedNumericDocValuesField("counter", Double.doubleToLongBits(1_000_000.0d + i * 0.5d)));
            doc.add(new SortedNumericDocValuesField("histogram", Double.doubleToLongBits(Math.log1p(i))));
            doc.add(new SortedNumericDocValuesField("position", Double.doubleToLongBits(i * 0.001d)));
            doc.add(new SortedNumericDocValuesField("no_role", Double.doubleToLongBits(i - 1024.0d)));
            writer.addDocument(doc);
        }, leaf -> {
            final SortedNumericDocValues timestamps = leaf.reader().getSortedNumericDocValues(TIMESTAMP_FIELD);
            final SortedNumericDocValues gauge = leaf.reader().getSortedNumericDocValues("gauge");
            final SortedNumericDocValues counter = leaf.reader().getSortedNumericDocValues("counter");
            final SortedNumericDocValues histogram = leaf.reader().getSortedNumericDocValues("histogram");
            final SortedNumericDocValues position = leaf.reader().getSortedNumericDocValues("position");
            final SortedNumericDocValues noRole = leaf.reader().getSortedNumericDocValues("no_role");
            for (int d = 0; d < NUM_DOCS; d++) {
                assertTrue(timestamps.advanceExact(d));
                final int i = (int) ((timestamps.nextValue() - 1_700_000_000_000L) / 1_000L);
                assertTrue(gauge.advanceExact(d));
                assertEquals("doc " + d + " gauge", Double.doubleToLongBits(Math.sin(i) * 100.0d), gauge.nextValue());
                assertTrue(counter.advanceExact(d));
                assertEquals("doc " + d + " counter", Double.doubleToLongBits(1_000_000.0d + i * 0.5d), counter.nextValue());
                assertTrue(histogram.advanceExact(d));
                assertEquals("doc " + d + " histogram", Double.doubleToLongBits(Math.log1p(i)), histogram.nextValue());
                assertTrue(position.advanceExact(d));
                assertEquals("doc " + d + " position", Double.doubleToLongBits(i * 0.001d), position.nextValue());
                assertTrue(noRole.advanceExact(d));
                assertEquals("doc " + d + " no_role", Double.doubleToLongBits(i - 1024.0d), noRole.nextValue());
            }
        });
    }

    public void testLongCounterAndDoubleCounterMixedRoundTrip() throws IOException {
        final FieldContextResolver resolver = (fieldName, defaultBlockSize) -> {
            final PipelineDescriptor.DataType dataType = fieldName.startsWith("long")
                ? PipelineDescriptor.DataType.LONG
                : PipelineDescriptor.DataType.DOUBLE;
            return new FieldContext(defaultBlockSize, fieldName, dataType, MetricRole.COUNTER);
        };
        indexAndVerify(resolver, true, (writer, i) -> {
            final Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, 1_700_000_000_000L + (long) i * 1_000L));
            doc.add(new SortedNumericDocValuesField("long_counter", 1_000_000L + (long) i * 7L));
            doc.add(new SortedNumericDocValuesField("double_counter", Double.doubleToLongBits(1_000_000.0d + i * 0.5d)));
            writer.addDocument(doc);
        }, leaf -> {
            final SortedNumericDocValues timestamps = leaf.reader().getSortedNumericDocValues(TIMESTAMP_FIELD);
            final SortedNumericDocValues longCounter = leaf.reader().getSortedNumericDocValues("long_counter");
            final SortedNumericDocValues doubleCounter = leaf.reader().getSortedNumericDocValues("double_counter");
            for (int d = 0; d < NUM_DOCS; d++) {
                assertTrue(timestamps.advanceExact(d));
                final int i = (int) ((timestamps.nextValue() - 1_700_000_000_000L) / 1_000L);
                assertTrue(longCounter.advanceExact(d));
                assertEquals("doc " + d + " long_counter", 1_000_000L + (long) i * 7L, longCounter.nextValue());
                assertTrue(doubleCounter.advanceExact(d));
                assertEquals("doc " + d + " double_counter", Double.doubleToLongBits(1_000_000.0d + i * 0.5d), doubleCounter.nextValue());
            }
        });
    }

    public void testMixedNumericAndSortedNumericRoundTrip() throws IOException {
        indexAndVerify(doubleResolver(name -> MetricRole.GAUGE), false, (writer, i) -> {
            final Document doc = new Document();
            doc.add(new NumericDocValuesField("single", Double.doubleToLongBits(i * 0.5d + 1.25d)));
            doc.add(new SortedNumericDocValuesField("multi", Double.doubleToLongBits(i * 0.25d - 100.0d)));
            writer.addDocument(doc);
        }, leaf -> {
            final NumericDocValues single = leaf.reader().getNumericDocValues("single");
            final SortedNumericDocValues multi = leaf.reader().getSortedNumericDocValues("multi");
            assertNotNull(single);
            assertNotNull(multi);
            for (int i = 0; i < NUM_DOCS; i++) {
                assertTrue("missing single doc " + i, single.advanceExact(i));
                assertEquals("single doc " + i, Double.doubleToLongBits(i * 0.5d + 1.25d), single.longValue());
                assertTrue("missing multi doc " + i, multi.advanceExact(i));
                assertEquals(1, multi.docValueCount());
                assertEquals("multi doc " + i, Double.doubleToLongBits(i * 0.25d - 100.0d), multi.nextValue());
            }
        });
    }

    private static void assertRoundTrip(final FieldKind kind, final MetricRole metricRole, final IntToDoubleFunction valueFn)
        throws IOException {
        assertRoundTrip(kind, metricRole, false, valueFn);
    }

    private static void assertRoundTrip(
        final FieldKind kind,
        final MetricRole metricRole,
        final boolean sortableLong,
        final IntToDoubleFunction valueFn
    ) throws IOException {
        indexAndVerify(doubleResolver(name -> metricRole), false, (writer, i) -> {
            final Document doc = new Document();
            final long encoded = encode(valueFn.applyAsDouble(i), sortableLong);
            doc.add(
                kind == FieldKind.NUMERIC ? new NumericDocValuesField(FIELD, encoded) : new SortedNumericDocValuesField(FIELD, encoded)
            );
            writer.addDocument(doc);
        }, leaf -> {
            if (kind == FieldKind.NUMERIC) {
                final NumericDocValues values = leaf.reader().getNumericDocValues(FIELD);
                assertNotNull(values);
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertTrue("missing doc " + i + " role=" + metricRole + " kind=" + kind, values.advanceExact(i));
                    final long expected = encode(valueFn.applyAsDouble(i), sortableLong);
                    assertEquals("role=" + metricRole + " doc=" + i, expected, values.longValue());
                }
            } else {
                final SortedNumericDocValues values = leaf.reader().getSortedNumericDocValues(FIELD);
                assertNotNull(values);
                for (int i = 0; i < NUM_DOCS; i++) {
                    assertTrue("missing doc " + i + " role=" + metricRole + " kind=" + kind, values.advanceExact(i));
                    assertEquals(1, values.docValueCount());
                    final long expected = encode(valueFn.applyAsDouble(i), sortableLong);
                    assertEquals("role=" + metricRole + " doc=" + i, expected, values.nextValue());
                }
            }
        });
    }

    private static void indexAndVerify(
        final FieldContextResolver resolver,
        final boolean indexSortByTimestamp,
        final CheckedBiConsumer<IndexWriter, Integer> writeDoc,
        final CheckedConsumer<LeafReaderContext, IOException> verifyLeaf
    ) throws IOException {
        final Codec codec = codecFor(resolver);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogByteSizeMergePolicy());
        if (indexSortByTimestamp) {
            config.setIndexSort(new Sort(new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, false)));
        }
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                writeDoc.accept(writer, i);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                verifyLeaf.accept(reader.leaves().get(0));
            }
        }
    }

    private static FieldContextResolver doubleResolver(final Function<String, MetricRole> roleFor) {
        return (fieldName, defaultBlockSize) -> new FieldContext(
            defaultBlockSize,
            fieldName,
            PipelineDescriptor.DataType.DOUBLE,
            roleFor.apply(fieldName)
        );
    }

    private static Codec codecFor(final FieldContextResolver resolver) {
        final DocValuesFormat docValuesFormat = ES95TSDBDocValuesFormatFactory.create(true, false, false, resolver);
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
    }

    private static long encode(double value, boolean sortableLong) {
        return sortableLong ? NumericUtils.doubleToSortableLong(value) : Double.doubleToLongBits(value);
    }

    private static double edgeValueOrIndex(int i) {
        return switch (i) {
            case 0 -> 0.0d;
            case 1 -> -0.0d;
            case 2 -> Double.MIN_VALUE;
            case 3 -> -Double.MIN_VALUE;
            case 4 -> Double.MIN_NORMAL;
            case 5 -> -Double.MIN_NORMAL;
            case 6 -> Double.MAX_VALUE;
            case 7 -> -Double.MAX_VALUE;
            case 8 -> Double.POSITIVE_INFINITY;
            case 9 -> Double.NEGATIVE_INFINITY;
            case 10 -> Double.NaN;
            default -> (double) i;
        };
    }

    @FunctionalInterface
    private interface CheckedBiConsumer<T, U> {
        void accept(T t, U u) throws IOException;
    }
}
