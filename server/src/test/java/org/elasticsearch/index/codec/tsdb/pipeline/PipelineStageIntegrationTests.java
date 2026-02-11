/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;

public class PipelineStageIntegrationTests extends NumericPipelineTestCase {

    public void testBitPackOnlyPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).bitPack()));
    }

    public void testZstdOnlyPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).zstd()));
    }

    public void testDeltaBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().bitPack()));
    }

    public void testDeltaOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack())
        );
    }

    public void testDeltaOffsetGcdZstdPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().zstd())
        );
    }

    public void testOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).offset().gcd().bitPack()));
    }

    public void testXorBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).xor().bitPack()));
    }

    public void testRleBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).rle().bitPack()));
    }

    public void testDeltaDeltaBitPackPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).deltaDelta().bitPack()));
    }

    public void testPatchedPForPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().patchedPFor().delta().bitPack())
        );
    }

    public void testAllTransformationStagesPipeline() throws IOException {
        assertRoundTripWithAllDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().patchedPFor().bitPack())
        );
    }

    public void testAlpDoubleStagePayloadPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().alpDouble())
        );
    }

    public void testAlpRdDoubleStagePayloadPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpRdDoubleStage().alpRdDouble())
        );
    }

    public void testAlpDoubleStageOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().offset().gcd().bitPack())
        );
    }

    public void testAlpRdDoubleStageOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpRdDoubleStage().offset().gcd().bitPack())
        );
    }

    public void testAlpDoubleStageGcdBitPackPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().gcd().bitPack())
        );
    }

    public void testAlpDoubleStageZstdPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).alpDoubleStage().zstd()));
    }

    public void testFpcDoubleStageOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithDoubleDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forDoubles(randomBlockSize()).fpcStage().offset().gcd().bitPack())
        );
    }

    public void testAlpFloatStagePayloadPipeline() throws IOException {
        assertRoundTripWithFloatDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forFloats(randomBlockSize()).alpFloatStage().alpFloat())
        );
    }

    public void testAlpRdFloatStagePayloadPipeline() throws IOException {
        assertRoundTripWithFloatDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forFloats(randomBlockSize()).alpRdFloatStage().alpRdFloat())
        );
    }

    public void testAlpFloatStageOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithFloatDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forFloats(randomBlockSize()).alpFloatStage().offset().gcd().bitPack())
        );
    }

    public void testAlpRdFloatStageOffsetGcdBitPackPipeline() throws IOException {
        assertRoundTripWithFloatDataTypes(
            NumericEncoder.fromConfig(PipelineConfig.forFloats(randomBlockSize()).alpRdFloatStage().offset().gcd().bitPack())
        );
    }

    public void testMultipleBlocksWithDeltaBitPack() throws IOException {
        assertMultipleBlocks(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().bitPack()));
    }

    public void testMultipleBlocksWithDeltaOffsetGcdBitPack() throws IOException {
        assertMultipleBlocks(NumericEncoder.fromConfig(PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack()));
    }
}
