/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar.storage;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetricRole;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor.DataType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;

/**
 * Pins the storage footprint of {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} across
 * eight synthetic workloads and logs a three-codec comparison against
 * {@link org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat} and Lucene90. Each test
 * method asserts an exact ColumNAR byte count; the output is deterministic because the workload
 * uses a fixed seed, the directory is in-memory ({@link org.apache.lucene.store.ByteBuffersDirectory}),
 * and documents are force-merged to a single segment before measurement.
 *
 * <p>Both ColumNAR and ES95 run at block size 128 (ColumNAR's current fixed block size) so the
 * comparison is purely algorithmic.
 */
public class ColumnarNumericFootprintTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(ColumnarNumericFootprintTests.class);
    private static final String FIELD = "value";
    private static final int DOC_COUNT = 50_000;
    private static final FieldType COLUMNAR_FIELD_TYPE = columnarFieldType();

    public void testFootprintMonotonicTimestamps() throws IOException {
        runFootprintTest("MONOTONIC_TIMESTAMPS", (f, bs) -> NumericPipeline.monotonicLongPipeline(bs), 71884);
    }

    public void testFootprintTsdbSplit() throws IOException {
        runFootprintTest("TSDB_SPLIT", (f, bs) -> NumericPipeline.monotonicLongPipeline(bs), 10326);
    }

    public void testFootprintCounterSteady() throws IOException {
        runFootprintTest("COUNTER_STEADY", (f, bs) -> NumericPipeline.defaultPipeline(bs), 9510);
    }

    public void testFootprintGauge() throws IOException {
        runFootprintTest("GAUGE", (f, bs) -> NumericPipeline.defaultPipeline(bs), 58590);
    }

    public void testFootprintSensorDoubles() throws IOException {
        runFootprintTest("SENSOR_DOUBLES", (f, bs) -> NumericPipeline.doubleGaugePipeline(bs), 63394);
    }

    public void testFootprintDoubleGauge() throws IOException {
        runFootprintTest("DOUBLE_GAUGE", (f, bs) -> NumericPipeline.doubleGaugePipeline(bs), 129617);
    }

    public void testFootprintDoubleCounter() throws IOException {
        runFootprintTest("DOUBLE_COUNTER", (f, bs) -> NumericPipeline.doubleCounterPipeline(bs), 265877);
    }

    public void testFootprintRandomFull() throws IOException {
        runFootprintTest("RANDOM_FULL", (f, bs) -> NumericPipeline.defaultPipeline(bs), 407362);
    }

    private void runFootprintTest(String workload, NumericPipelineSelector columnarPipeline, long expectedBytes) throws IOException {
        final long[] values = generate(workload, DOC_COUNT);
        final long lucene = measureBytes(new Lucene90DocValuesFormat(), values, false);
        final long es95 = measureBytes(es95Format(workload), values, false);
        final long columnar = measureBytes(new ColumNARDocValuesFormat(columnarPipeline), values, true);
        logger.info(
            "workload={} docs={} lucene={} es95={} columnar={} columnar/es95={}x columnar/lucene={}x",
            workload,
            DOC_COUNT,
            lucene,
            es95,
            columnar,
            String.format(Locale.ROOT, "%.2f", (double) columnar / es95),
            String.format(Locale.ROOT, "%.2f", (double) columnar / lucene)
        );
        assertEquals(workload + ": unexpected ColumNAR byte count", expectedBytes, columnar);
    }

    private static DocValuesFormat es95Format(String workload) {
        // NOTE: ColumNAR is currently fixed at block size 128. ES95 is configured here to match
        // that block size (useLargeNumericBlockSize=false) so the comparison is purely algorithmic.
        // Storage results at block size 512 (TSDB production default) will be recaptured once
        // ColumNAR exposes a configurable block size.
        return ES95TSDBDocValuesFormatFactory.create(false, false, false, (f, bs) -> switch (workload) {
            case "SENSOR_DOUBLES", "DOUBLE_GAUGE" -> new FieldContext(bs, f, DataType.DOUBLE, MetricRole.GAUGE);
            case "DOUBLE_COUNTER" -> new FieldContext(bs, f, DataType.DOUBLE, MetricRole.COUNTER);
            case "COUNTER_STEADY" -> new FieldContext(bs, f, DataType.LONG, MetricRole.COUNTER);
            case "GAUGE" -> new FieldContext(bs, f, DataType.LONG, MetricRole.GAUGE);
            // NOTE: TSDB_SPLIT and MONOTONIC_TIMESTAMPS represent timestamp-like data that uses
            // the splitDelta pipeline in production (field named "@timestamp"). Passing that name
            // here gives ES95 the same pipeline as ColumNAR's monotonicLongPipeline, making the
            // comparison fair. Without it, ES95 falls back to the baseline (no splitDelta) and
            // produces ~234-byte boundary blocks instead of ~18 bytes.
            case "TSDB_SPLIT", "MONOTONIC_TIMESTAMPS" -> new FieldContext(bs, "@timestamp", null, null);
            default -> new FieldContext(bs, f, null, null);
        });
    }

    private long measureBytes(DocValuesFormat dvFormat, long[] values, boolean columnar) throws IOException {
        final Codec codec = new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dvFormat;
            }
        };
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec);
            final BytesRefBuilder builder = new BytesRefBuilder();
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (long value : values) {
                    final Document doc = new Document();
                    if (columnar) {
                        doc.add(
                            new Field(
                                FIELD,
                                BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder)),
                                COLUMNAR_FIELD_TYPE
                            )
                        );
                    } else {
                        doc.add(new SortedNumericDocValuesField(FIELD, value));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            long total = 0;
            for (String file : dir.listAll()) {
                total += dir.fileLength(file);
            }
            return total;
        }
    }

    private static long[] generate(String workload, int count) {
        final Random rng = new Random(42L);
        final long[] values = new long[count];
        long timestamp = 1_700_000_000_000L;
        double counterValue = 0.0;
        for (int i = 0; i < count; i++) {
            values[i] = switch (workload) {
                case "MONOTONIC_TIMESTAMPS" -> {
                    timestamp += 1 + rng.nextInt(1000);
                    yield timestamp;
                }
                case "TSDB_SPLIT" -> {
                    int runsOf = Math.max(1, count / 4);
                    yield 1_700_000_000_000L + (long) (i / runsOf) * 100_000L - (long) (i % runsOf) * 1_000L;
                }
                case "COUNTER_STEADY" -> 1000L * i;
                case "GAUGE" -> 50_000_000L + rng.nextInt(201) - 100;
                case "SENSOR_DOUBLES" -> NumericUtils.doubleToSortableLong(20.0 + (i % 1000) * 0.1);
                case "DOUBLE_GAUGE" -> NumericUtils.doubleToSortableLong(50.0 + (rng.nextInt(201) - 100) * 0.01);
                case "DOUBLE_COUNTER" -> {
                    counterValue += 1.5 + rng.nextInt(100) * 0.001;
                    yield NumericUtils.doubleToSortableLong(counterValue);
                }
                case "RANDOM_FULL" -> rng.nextLong();
                default -> throw new IllegalArgumentException("Unknown workload: " + workload);
            };
        }
        return values;
    }

    private static FieldType columnarFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.LONG.name());
        type.freeze();
        return type;
    }
}
