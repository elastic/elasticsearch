/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.DoubleFunction;

public class StageSpecTests extends ESTestCase {

    private static final List<DoubleFunction<StageSpec>> MAX_ERROR_CONSTRUCTORS = List.of(
        StageSpec.AlpDoubleStage::new,
        StageSpec.AlpFloatStage::new,
        StageSpec.GorillaDoublePayload::new,
        StageSpec.ChimpDoublePayload::new,
        StageSpec.Chimp128DoublePayload::new
    );

    public void testMaxErrorRejectsNegative() {
        final double negative = -randomDoubleBetween(Double.MIN_VALUE, 1000, true);
        for (final DoubleFunction<StageSpec> ctor : MAX_ERROR_CONSTRUCTORS) {
            expectThrows(IllegalArgumentException.class, () -> ctor.apply(negative));
        }
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcDoubleStage(randomIntBetween(0, 2048), negative));
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcFloatStage(randomIntBetween(0, 2048), negative));
    }

    public void testMaxErrorRejectsNaN() {
        for (final DoubleFunction<StageSpec> ctor : MAX_ERROR_CONSTRUCTORS) {
            expectThrows(IllegalArgumentException.class, () -> ctor.apply(Double.NaN));
        }
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcDoubleStage(randomIntBetween(0, 2048), Double.NaN));
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcFloatStage(randomIntBetween(0, 2048), Double.NaN));
    }

    public void testMaxErrorRejectsInfinity() {
        final double inf = randomBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        for (final DoubleFunction<StageSpec> ctor : MAX_ERROR_CONSTRUCTORS) {
            expectThrows(IllegalArgumentException.class, () -> ctor.apply(inf));
        }
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcDoubleStage(randomIntBetween(0, 2048), inf));
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcFloatStage(randomIntBetween(0, 2048), inf));
    }

    public void testMaxErrorAcceptsValidValues() {
        final double valid = randomDoubleBetween(0, 1000, true);
        final int tableSize = randomIntBetween(0, 2048);
        for (final DoubleFunction<StageSpec> ctor : MAX_ERROR_CONSTRUCTORS) {
            ctor.apply(valid);
        }
        new StageSpec.FpcDoubleStage(tableSize, valid);
        new StageSpec.FpcFloatStage(tableSize, valid);
    }

    public void testTableSizeRejectsNegative() {
        final int negative = -randomIntBetween(1, 10000);
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcDoubleStage(negative));
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.FpcFloatStage(negative));
    }

    public void testTableSizeAcceptsValid() {
        final int valid = randomIntBetween(0, 10000);
        assertEquals(valid, new StageSpec.FpcDoubleStage(valid).tableSize());
        assertEquals(valid, new StageSpec.FpcFloatStage(valid).tableSize());
    }

    public void testCompressionLevelRejectsOutOfRange() {
        final int tooLow = randomIntBetween(Integer.MIN_VALUE, StageSpec.ZstdPayload.MIN_COMPRESSION_LEVEL - 1);
        final int tooHigh = randomIntBetween(StageSpec.ZstdPayload.MAX_COMPRESSION_LEVEL + 1, Integer.MAX_VALUE);
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.ZstdPayload(tooLow));
        expectThrows(IllegalArgumentException.class, () -> new StageSpec.ZstdPayload(tooHigh));
    }

    public void testCompressionLevelAcceptsValid() {
        final int valid = randomIntBetween(StageSpec.ZstdPayload.MIN_COMPRESSION_LEVEL, StageSpec.ZstdPayload.MAX_COMPRESSION_LEVEL);
        assertEquals(valid, new StageSpec.ZstdPayload(valid).compressionLevel());
    }

    public void testCompressionLevelDefaultValue() {
        assertEquals(StageSpec.ZstdPayload.DEFAULT_COMPRESSION_LEVEL, new StageSpec.ZstdPayload().compressionLevel());
    }

    public void testAllStageSpecsHaveStageIds() {
        assertNotNull(new StageSpec.DeltaStage().stageId());
        assertNotNull(new StageSpec.OffsetStage().stageId());
        assertNotNull(new StageSpec.GcdStage().stageId());
        assertNotNull(new StageSpec.PatchedPForStage().stageId());
        assertNotNull(new StageSpec.XorStage().stageId());
        assertNotNull(new StageSpec.AlpDoubleStage().stageId());
        assertNotNull(new StageSpec.AlpFloatStage().stageId());
        assertNotNull(new StageSpec.FpcDoubleStage().stageId());
        assertNotNull(new StageSpec.FpcFloatStage().stageId());
        assertNotNull(new StageSpec.BitPackPayload().stageId());
        assertNotNull(new StageSpec.ZstdPayload().stageId());
        assertNotNull(new StageSpec.Lz4Payload().stageId());
        assertNotNull(new StageSpec.GorillaDoublePayload().stageId());
        assertNotNull(new StageSpec.GorillaFloatPayload().stageId());
        assertNotNull(new StageSpec.ChimpDoublePayload().stageId());
        assertNotNull(new StageSpec.ChimpFloatPayload().stageId());
        assertNotNull(new StageSpec.Chimp128DoublePayload().stageId());
        assertNotNull(new StageSpec.Chimp128FloatPayload().stageId());
    }
}
