/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.test.ESTestCase;

public class StageEqualsHashCodeToStringTests extends ESTestCase {

    private static final int BLOCK_SIZE = 128;

    public void testGcdCodecStageToString() {
        assertEquals("GcdCodecStage", GcdCodecStage.INSTANCE.toString());
    }

    public void testXorCodecStageToString() {
        assertEquals("XorCodecStage", XorCodecStage.INSTANCE.toString());
    }

    public void testFpcDoubleTransformDecodeStageToString() {
        assertEquals("FpcDoubleTransformDecodeStage{tableSize=1024}", new FpcDoubleTransformDecodeStage(512).toString());
    }

    public void testFpcDoubleTransformDecodeStageEqualsHashCode() {
        final var a = new FpcDoubleTransformDecodeStage(512, 1024);
        final var b = new FpcDoubleTransformDecodeStage(512, 1024);
        final var c = new FpcDoubleTransformDecodeStage(512, 512);
        assertEqualsContract(a, b, c);
    }

    public void testFpcFloatTransformDecodeStageToString() {
        assertEquals("FpcFloatTransformDecodeStage{tableSize=1024}", new FpcFloatTransformDecodeStage(512).toString());
    }

    public void testFpcFloatTransformDecodeStageEqualsHashCode() {
        final var a = new FpcFloatTransformDecodeStage(512, 1024);
        final var b = new FpcFloatTransformDecodeStage(512, 1024);
        final var c = new FpcFloatTransformDecodeStage(512, 512);
        assertEqualsContract(a, b, c);
    }

    public void testDeltaDeltaCodecStageToString() {
        assertEquals("DeltaDeltaCodecStage", DeltaDeltaCodecStage.INSTANCE.toString());
    }

    public void testRlePayloadCodecStageToString() {
        assertEquals("RlePayloadCodecStage", RlePayloadCodecStage.INSTANCE.toString());
    }

    public void testRleDecodeStageToString() {
        assertEquals("RleDecodeStage", new RleDecodeStage().toString());
    }

    public void testPatchedPForDecodeStageToString() {
        assertEquals("PatchedPForDecodeStage", new PatchedPForDecodeStage().toString());
    }

    public void testAlpDoubleTransformDecodeStageToString() {
        assertEquals("AlpDoubleTransformDecodeStage", new AlpDoubleTransformDecodeStage().toString());
    }

    public void testAlpFloatTransformDecodeStageToString() {
        assertEquals("AlpFloatTransformDecodeStage", new AlpFloatTransformDecodeStage().toString());
    }

    public void testAlpRdDoubleTransformDecodeStageToString() {
        assertEquals("AlpRdDoubleTransformDecodeStage", new AlpRdDoubleTransformDecodeStage().toString());
    }

    public void testAlpRdFloatTransformDecodeStageToString() {
        assertEquals("AlpRdFloatTransformDecodeStage", new AlpRdFloatTransformDecodeStage().toString());
    }

    public void testGorillaEncodeStageToString() {
        assertEquals("GorillaEncodeStage", new GorillaEncodeStage().toString());
    }

    public void testGorillaDecodeStageToString() {
        assertEquals("GorillaDecodeStage", new GorillaDecodeStage().toString());
    }

    public void testDeltaCodecStageEqualsHashCode() {
        final var a = new DeltaCodecStage(3);
        final var b = new DeltaCodecStage(3);
        final var c = new DeltaCodecStage(5);
        assertEqualsContract(a, b, c);
    }

    public void testDeltaCodecStageToString() {
        assertEquals("DeltaCodecStage{minDirectionalChanges=2}", new DeltaCodecStage().toString());
    }

    public void testOffsetCodecStageEqualsHashCode() {
        final var a = new OffsetCodecStage(30);
        final var b = new OffsetCodecStage(30);
        final var c = new OffsetCodecStage(50);
        assertEqualsContract(a, b, c);
    }

    public void testOffsetCodecStageToString() {
        assertEquals("OffsetCodecStage{minOffsetRatioPercent=25}", new OffsetCodecStage().toString());
    }

    public void testQuantizeDoubleCodecStageEqualsHashCode() {
        final var a = new QuantizeDoubleCodecStage(1e-6);
        final var b = new QuantizeDoubleCodecStage(1e-6);
        final var c = new QuantizeDoubleCodecStage(1e-3);
        assertEqualsContract(a, b, c);
    }

    public void testQuantizeDoubleCodecStageToString() {
        assertEquals("QuantizeDoubleCodecStage{maxError=1.0E-6}", new QuantizeDoubleCodecStage(1e-6).toString());
    }

    public void testPatchedPForEncodeStageEqualsHashCode() {
        final var a = new PatchedPForEncodeStage(10);
        final var b = new PatchedPForEncodeStage(10);
        final var c = new PatchedPForEncodeStage(20);
        assertEqualsContract(a, b, c);
    }

    public void testPatchedPForEncodeStageToString() {
        assertEquals("PatchedPForEncodeStage{maxExceptionPercent=10}", new PatchedPForEncodeStage().toString());
    }

    public void testRleEncodeStageEqualsHashCode() {
        final var a = new RleEncodeStage(BLOCK_SIZE, 0.90);
        final var b = new RleEncodeStage(BLOCK_SIZE, 0.90);
        final var c = new RleEncodeStage(BLOCK_SIZE, 0.50);
        assertEqualsContract(a, b, c);
    }

    public void testRleEncodeStageToString() {
        assertEquals("RleEncodeStage{maxRunRatioThreshold=0.9}", new RleEncodeStage(BLOCK_SIZE).toString());
    }

    public void testBitPackCodecStageEqualsHashCode() {
        final var a = new BitPackCodecStage(128);
        final var b = new BitPackCodecStage(128);
        final var c = new BitPackCodecStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testBitPackCodecStageToString() {
        assertEquals("BitPackCodecStage{blockSize=128}", new BitPackCodecStage(128).toString());
    }

    public void testFpcDoubleTransformEncodeStageEqualsHashCode() {
        final var a = new FpcDoubleTransformEncodeStage(BLOCK_SIZE, 1024);
        final var b = new FpcDoubleTransformEncodeStage(BLOCK_SIZE, 1024);
        final var c = new FpcDoubleTransformEncodeStage(BLOCK_SIZE, 512);
        assertEqualsContract(a, b, c);
        final var d = new FpcDoubleTransformEncodeStage(BLOCK_SIZE, 1024, 1e-3);
        assertNotEquals(a, d);
    }

    public void testFpcDoubleTransformEncodeStageToString() {
        assertEquals(
            "FpcDoubleTransformEncodeStage{tableSize=1024, quantizeStep=0.0}",
            new FpcDoubleTransformEncodeStage(BLOCK_SIZE).toString()
        );
        assertEquals(
            "FpcDoubleTransformEncodeStage{tableSize=1024, quantizeStep=0.002}",
            new FpcDoubleTransformEncodeStage(BLOCK_SIZE, 1024, 1e-3).toString()
        );
    }

    public void testFpcFloatTransformEncodeStageEqualsHashCode() {
        final var a = new FpcFloatTransformEncodeStage(BLOCK_SIZE, 1024);
        final var b = new FpcFloatTransformEncodeStage(BLOCK_SIZE, 1024);
        final var c = new FpcFloatTransformEncodeStage(BLOCK_SIZE, 512);
        assertEqualsContract(a, b, c);
        final var d = new FpcFloatTransformEncodeStage(BLOCK_SIZE, 1024, 1e-3);
        assertNotEquals(a, d);
    }

    public void testFpcFloatTransformEncodeStageToString() {
        assertEquals(
            "FpcFloatTransformEncodeStage{tableSize=1024, quantizeStep=0.0}",
            new FpcFloatTransformEncodeStage(BLOCK_SIZE).toString()
        );
        assertEquals(
            "FpcFloatTransformEncodeStage{tableSize=1024, quantizeStep=0.002}",
            new FpcFloatTransformEncodeStage(BLOCK_SIZE, 1024, 1e-3).toString()
        );
    }

    public void testZstdEncodeStageEqualsHashCode() {
        final var a = new ZstdEncodeStage(128, 1);
        final var b = new ZstdEncodeStage(128, 1);
        final var c = new ZstdEncodeStage(128, 3);
        assertEqualsContract(a, b, c);
        final var d = new ZstdEncodeStage(256, 1);
        assertNotEquals(a, d);
    }

    public void testZstdEncodeStageToString() {
        assertEquals("ZstdEncodeStage{compressionLevel=1, blockSize=128}", new ZstdEncodeStage(128, 1).toString());
    }

    public void testZstdDecodeStageEqualsHashCode() {
        final var a = new ZstdDecodeStage(128);
        final var b = new ZstdDecodeStage(128);
        final var c = new ZstdDecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testZstdDecodeStageToString() {
        assertEquals("ZstdDecodeStage{blockSize=128}", new ZstdDecodeStage(128).toString());
    }

    public void testAlpDoubleTransformEncodeStageEqualsHashCode() {
        final var a = new AlpDoubleTransformEncodeStage(BLOCK_SIZE);
        final var b = new AlpDoubleTransformEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpDoubleTransformEncodeStage(BLOCK_SIZE, 1e-6);
        assertNotEquals(a, c);
    }

    public void testAlpDoubleTransformEncodeStageToString() {
        assertThat(
            new AlpDoubleTransformEncodeStage(BLOCK_SIZE).toString(),
            org.hamcrest.Matchers.startsWith("AlpDoubleTransformEncodeStage{")
        );
    }

    public void testAlpRdDoubleTransformEncodeStageEqualsHashCode() {
        final var a = new AlpRdDoubleTransformEncodeStage(BLOCK_SIZE);
        final var b = new AlpRdDoubleTransformEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpRdDoubleTransformEncodeStage(BLOCK_SIZE, 1e-6);
        assertNotEquals(a, c);
    }

    public void testAlpRdDoubleTransformEncodeStageToString() {
        assertThat(
            new AlpRdDoubleTransformEncodeStage(BLOCK_SIZE).toString(),
            org.hamcrest.Matchers.startsWith("AlpRdDoubleTransformEncodeStage{")
        );
    }

    public void testAlpFloatTransformEncodeStageEqualsHashCode() {
        final var a = new AlpFloatTransformEncodeStage(BLOCK_SIZE);
        final var b = new AlpFloatTransformEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpFloatTransformEncodeStage(BLOCK_SIZE, 1e-3);
        assertNotEquals(a, c);
    }

    public void testAlpFloatTransformEncodeStageToString() {
        assertThat(
            new AlpFloatTransformEncodeStage(BLOCK_SIZE).toString(),
            org.hamcrest.Matchers.startsWith("AlpFloatTransformEncodeStage{")
        );
    }

    public void testAlpRdFloatTransformEncodeStageEqualsHashCode() {
        final var a = new AlpRdFloatTransformEncodeStage(BLOCK_SIZE);
        final var b = new AlpRdFloatTransformEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpRdFloatTransformEncodeStage(BLOCK_SIZE, 1e-3);
        assertNotEquals(a, c);
    }

    public void testAlpRdFloatTransformEncodeStageToString() {
        assertThat(
            new AlpRdFloatTransformEncodeStage(BLOCK_SIZE).toString(),
            org.hamcrest.Matchers.startsWith("AlpRdFloatTransformEncodeStage{")
        );
    }

    public void testAlpDoubleEncodeStageEqualsHashCode() {
        final var a = new AlpDoubleEncodeStage(BLOCK_SIZE);
        final var b = new AlpDoubleEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpDoubleEncodeStage(256);
        assertNotEquals(a, c);
    }

    public void testAlpDoubleEncodeStageToString() {
        assertThat(new AlpDoubleEncodeStage(BLOCK_SIZE).toString(), org.hamcrest.Matchers.startsWith("AlpDoubleEncodeStage{"));
    }

    public void testAlpRdDoubleEncodeStageEqualsHashCode() {
        final var a = new AlpRdDoubleEncodeStage(BLOCK_SIZE);
        final var b = new AlpRdDoubleEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpRdDoubleEncodeStage(256);
        assertNotEquals(a, c);
    }

    public void testAlpRdDoubleEncodeStageToString() {
        assertThat(new AlpRdDoubleEncodeStage(BLOCK_SIZE).toString(), org.hamcrest.Matchers.startsWith("AlpRdDoubleEncodeStage{"));
    }

    public void testAlpFloatEncodeStageEqualsHashCode() {
        final var a = new AlpFloatEncodeStage(BLOCK_SIZE);
        final var b = new AlpFloatEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpFloatEncodeStage(256);
        assertNotEquals(a, c);
    }

    public void testAlpFloatEncodeStageToString() {
        assertThat(new AlpFloatEncodeStage(BLOCK_SIZE).toString(), org.hamcrest.Matchers.startsWith("AlpFloatEncodeStage{"));
    }

    public void testAlpRdFloatEncodeStageEqualsHashCode() {
        final var a = new AlpRdFloatEncodeStage(BLOCK_SIZE);
        final var b = new AlpRdFloatEncodeStage(BLOCK_SIZE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        final var c = new AlpRdFloatEncodeStage(256);
        assertNotEquals(a, c);
    }

    public void testAlpRdFloatEncodeStageToString() {
        assertThat(new AlpRdFloatEncodeStage(BLOCK_SIZE).toString(), org.hamcrest.Matchers.startsWith("AlpRdFloatEncodeStage{"));
    }

    public void testAlpDoubleDecodeStageEqualsHashCode() {
        final var a = new AlpDoubleDecodeStage(128);
        final var b = new AlpDoubleDecodeStage(128);
        final var c = new AlpDoubleDecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testAlpDoubleDecodeStageToString() {
        assertEquals("AlpDoubleDecodeStage{blockSize=128}", new AlpDoubleDecodeStage(128).toString());
    }

    public void testAlpFloatDecodeStageEqualsHashCode() {
        final var a = new AlpFloatDecodeStage(128);
        final var b = new AlpFloatDecodeStage(128);
        final var c = new AlpFloatDecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testAlpFloatDecodeStageToString() {
        assertEquals("AlpFloatDecodeStage{blockSize=128}", new AlpFloatDecodeStage(128).toString());
    }

    public void testAlpRdDoubleDecodeStageEqualsHashCode() {
        final var a = new AlpRdDoubleDecodeStage(128);
        final var b = new AlpRdDoubleDecodeStage(128);
        final var c = new AlpRdDoubleDecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testAlpRdDoubleDecodeStageToString() {
        assertEquals("AlpRdDoubleDecodeStage{blockSize=128}", new AlpRdDoubleDecodeStage(128).toString());
    }

    public void testAlpRdFloatDecodeStageEqualsHashCode() {
        final var a = new AlpRdFloatDecodeStage(128);
        final var b = new AlpRdFloatDecodeStage(128);
        final var c = new AlpRdFloatDecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testAlpRdFloatDecodeStageToString() {
        assertEquals("AlpRdFloatDecodeStage{blockSize=128}", new AlpRdFloatDecodeStage(128).toString());
    }

    public void testLz4EncodeStageEqualsHashCode() {
        final var a = new Lz4EncodeStage(128, false);
        final var b = new Lz4EncodeStage(128, false);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new Lz4EncodeStage(128, true));
        assertNotEquals(a, new Lz4EncodeStage(256, false));
    }

    public void testLz4EncodeStageToString() {
        assertEquals("Lz4EncodeStage{highCompression=false, blockSize=128}", new Lz4EncodeStage(128).toString());
    }

    public void testLz4DecodeStageEqualsHashCode() {
        final var a = new Lz4DecodeStage(128);
        final var b = new Lz4DecodeStage(128);
        final var c = new Lz4DecodeStage(256);
        assertEqualsContract(a, b, c);
    }

    public void testLz4DecodeStageToString() {
        assertEquals("Lz4DecodeStage{blockSize=128}", new Lz4DecodeStage(128).toString());
    }

    public void testEqualsNullAndCrossType() {
        final var delta = new DeltaCodecStage();
        assertNotEquals(null, delta);
        assertNotEquals(new OffsetCodecStage(), delta);
        assertNotEquals("not a stage", delta);
    }

    private static void assertEqualsContract(Object a, Object equalToA, Object different) {
        assertEquals(a, a);
        assertEquals(a, equalToA);
        assertEquals(equalToA, a);
        assertEquals(a.hashCode(), equalToA.hashCode());
        assertNotEquals(a, different);
        assertNotEquals(null, a);
        assertNotNull(a.toString());
    }
}
