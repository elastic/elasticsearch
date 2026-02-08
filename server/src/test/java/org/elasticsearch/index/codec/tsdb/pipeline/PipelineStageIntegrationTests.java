/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.io.IOException;

public class PipelineStageIntegrationTests extends NumericPipelineTestCase {

    public void testDeltaBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().bitPack().build());
    }

    public void testDeltaOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().offset().gcd().bitPack().build()
        );
    }

    public void testDeltaOffsetGcdZstdPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().offset().gcd().zstd().build()
        );
    }

    public void testPatchedPForPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().patchedPFor().delta().bitPack().build()
        );
    }

    public void testDeltaZigzagBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().zigzag().bitPack().build()
        );
    }

    public void testMultipleBlocksWithDeltaBitPack() throws IOException {
        assertMultipleBlocks(NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).delta().bitPack().build());
    }

    public void testBitPackOnlyPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).bitPack().build());
    }

    public void testZstdOnlyPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE).zstd().build());
    }

    public void testAllTransformationStagesPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericCodec.withBlockSize(ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE)
                .delta()
                .offset()
                .gcd()
                .zigzag()
                .patchedPFor()
                .bitPack()
                .build()
        );
    }
}
