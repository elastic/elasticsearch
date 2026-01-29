/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public class PipelineCompressionTests extends NumericPipelineTestCase {

    public void testTimestampData() throws IOException {
        comparePipelines(
            "timestamp-with-jitter",
            () -> timestampWithJitterData(10),
            fullPipeline(),
            fullPipelineZstd(),
            deltaOffsetBitpack(),
            deltaOffsetZstd(),
            offsetBitpack(),
            offsetZstd()
        );
    }

    public void testGaugeData() throws IOException {
        comparePipelines(
            "gauge",
            this::gaugeData,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testCounterData() throws IOException {
        comparePipelines(
            "counter",
            this::counterData,
            fullPipeline(),
            fullPipelineZstd(),
            deltaOffsetBitpack(),
            deltaOffsetZstd(),
            offsetBitpack(),
            offsetZstd()
        );
    }

    public void testGcdData() throws IOException {
        comparePipelines(
            "gcd",
            this::gcdData,
            fullPipeline(),
            fullPipelineZstd(),
            offsetGcdBitpack(),
            offsetGcdZstd(),
            offsetBitpack(),
            offsetZstd()
        );
    }

    public void testConstantData() throws IOException {
        comparePipelines("constant", this::constantData, fullPipeline(), fullPipelineZstd(), offsetBitpack(), offsetZstd());
    }

    public void testRandomData() throws IOException {
        comparePipelines(
            "random",
            this::randomData,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testSmallData() throws IOException {
        comparePipelines(
            "small",
            this::smallData,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testSparseData() throws IOException {
        comparePipelines(
            "sparse",
            this::sparseData,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testDataWithOutliers() throws IOException {
        comparePipelines(
            "outliers",
            this::dataWithOutliers,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testGaugeWithOutliers() throws IOException {
        comparePipelines(
            "gauge-outliers",
            this::gaugeWithOutliers,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetPatchedPforBitpack(),
            offsetPatchedPforZstd()
        );
    }

    public void testRepeatedPatterns() throws IOException {
        comparePipelines(
            "repeated",
            this::dataWithRepeatedPatterns,
            fullPipeline(),
            fullPipelineZstd(),
            offsetBitpack(),
            offsetZstd(),
            offsetGcdBitpack(),
            offsetGcdZstd()
        );
    }

    public void testOscillatingData() throws IOException {
        comparePipelines(
            "oscillating",
            this::oscillatingData,
            fullPipeline(),
            fullPipelineZstd(),
            deltaOffsetBitpack(),
            deltaOffsetZstd(),
            offsetBitpack(),
            offsetZstd()
        );
    }

    private long[] dataWithOutliers() {
        final long[] values = new long[BLOCK_SIZE];
        final Random random = new Random(42);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            if (random.nextDouble() < 0.05) {
                values[i] = random.nextInt(1000000) + 1000000;
            } else {
                values[i] = random.nextInt(255);
            }
        }
        return values;
    }

    private long[] gaugeWithOutliers() {
        final long[] values = new long[BLOCK_SIZE];
        final Random random = new Random(42);
        final long baseline = 50000L;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            if (random.nextDouble() < 0.03) {
                values[i] = baseline + random.nextInt(100000);
            } else {
                values[i] = baseline + random.nextInt(1000) - 500;
            }
        }
        return values;
    }

    private long[] sparseData() {
        final long[] values = new long[BLOCK_SIZE];
        final Random random = new Random(42);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            if (random.nextDouble() < 0.1) {
                values[i] = random.nextInt(10000) + 1;
            } else {
                values[i] = 0;
            }
        }
        return values;
    }

    private long[] dataWithRepeatedPatterns() {
        final long[] values = new long[BLOCK_SIZE];
        final long[] patterns = { 100, 200, 300, 100, 200, 300, 400, 500 };
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = patterns[i % patterns.length];
        }
        return values;
    }

    private long[] oscillatingData() {
        final long[] values = new long[BLOCK_SIZE];
        final Random random = new Random(42);
        long current = 1000000L;
        for (int i = 0; i < BLOCK_SIZE; i++) {
            final int delta = random.nextInt(100) + 1;
            current += (i % 2 == 0) ? delta : -delta;
            values[i] = current;
        }
        return values;
    }

    private NumericCodec fullPipeline() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().gcd().bitPack().build();
    }

    private NumericCodec fullPipelineZstd() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().gcd().zstd().build();
    }

    private NumericCodec deltaOffsetBitpack() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().bitPack().build();
    }

    private NumericCodec deltaOffsetZstd() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().zstd().build();
    }

    private NumericCodec offsetBitpack() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    private NumericCodec offsetZstd() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().zstd().build();
    }

    private NumericCodec offsetGcdBitpack() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().gcd().bitPack().build();
    }

    private NumericCodec offsetGcdZstd() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().gcd().zstd().build();
    }

    private NumericCodec offsetPatchedPforBitpack() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().patchedPFor().bitPack().build();
    }

    private NumericCodec offsetPatchedPforZstd() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().patchedPFor().zstd().build();
    }

    private static final String ES87_PIPELINE = "delta->offset->gcd->bit-pack";

    private void comparePipelines(final String dataType, final Supplier<long[]> dataSupplier, final NumericCodec... pipelines)
        throws IOException {
        final long[] originalData = dataSupplier.get();
        final int originalSize = originalData.length * Long.BYTES;
        final int es87Size = encodeWithES87(originalData.clone());

        final List<PipelineResultTest> results = new ArrayList<>();
        for (final NumericCodec codec : pipelines) {
            final String pipeline = codec.toString();
            try (codec) {
                final EncodeResult result = encodeAndDecode(codec, originalData.clone());
                assertArrayEquals("Roundtrip failed for " + dataType + " with " + pipeline, originalData, result.decoded());
                results.add(new PipelineResultTest(pipeline, result.encodedSize(), countStages(pipeline)));
            }
        }

        results.sort(Comparator.comparingInt(r -> r.size));
        final int bestSize = results.getFirst().size;

        logger.info("original: {} bytes", originalSize);
        logger.info("ES87 [{}]: {} bytes (4 stages)", ES87_PIPELINE, es87Size);

        for (final PipelineResultTest r : results) {
            final int diff = r.size - es87Size;
            final String diffStr = (diff >= 0 ? "+" : "") + diff;
            final String betterThanES87 = diff < 0 ? "***" : diff == 0 ? "===" : "";
            if (r.size == bestSize) {
                logger.info("ES94 [{}]: {} bytes ({} stages, {} vs ES87), {}", r.pipeline, r.size, r.stages, diffStr, betterThanES87);
            } else {
                logger.info("ES94 [{}]: {} bytes ({} stages, {} vs ES87), {}", r.pipeline, r.size, r.stages, diffStr, betterThanES87);
            }
        }
    }

    private static int countStages(final String pipeline) {
        return pipeline.split("->").length;
    }

    private int encodeWithES87(final long[] values) throws IOException {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(BLOCK_SIZE);
        final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        encoder.encode(values, out);
        return out.getPosition();
    }

    private record PipelineResultTest(String pipeline, int size, int stages) {}
}
