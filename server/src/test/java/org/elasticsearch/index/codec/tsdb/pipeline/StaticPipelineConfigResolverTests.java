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

public class StaticPipelineConfigResolverTests extends ESTestCase {

    public void testTimestampFieldRoutesToDeltaOfDeltaPipeline() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), "@timestamp"));

        assertEquals(
            List.of(StageId.DELTA_OF_DELTA_STAGE, StageId.OFFSET_STAGE, StageId.BITPACK_PAYLOAD),
            config.specs().stream().map(StageSpec::stageId).toList()
        );
    }

    public void testNonTimestampFieldRoutesToBaselinePipeline() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), randomAlphaOfLengthBetween(1, 16)));

        assertEquals(
            List.of(StageId.DELTA_STAGE, StageId.OFFSET_STAGE, StageId.GCD_STAGE, StageId.BITPACK_PAYLOAD),
            config.specs().stream().map(StageSpec::stageId).toList()
        );
    }

    public void testPropagatesBlockSizeFromContext() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        assertEquals(blockSize, resolver.resolve(new FieldContext(blockSize, "@timestamp")).blockSize());
        assertEquals(blockSize, resolver.resolve(new FieldContext(blockSize, randomAlphaOfLengthBetween(1, 16))).blockSize());
    }

    public void testTimestampNameIsCaseSensitive() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), "@TIMESTAMP"));

        assertEquals(
            List.of(StageId.DELTA_STAGE, StageId.OFFSET_STAGE, StageId.GCD_STAGE, StageId.BITPACK_PAYLOAD),
            config.specs().stream().map(StageSpec::stageId).toList()
        );
    }

    public void testCachedBlockSizesReturnSameInstance() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, randomAlphaOfLengthBetween(1, 16)));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, randomAlphaOfLengthBetween(1, 16)));
        assertSame(first, second);
    }

    public void testTimestampCachedBlockSizesReturnSameInstance() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, "@timestamp"));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, "@timestamp"));
        assertSame(first, second);
    }

    public void testUncachedBlockSizeStillResolves() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = 1 << randomIntBetween(4, 6);

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomAlphaOfLengthBetween(1, 16)));
        assertEquals(blockSize, config.blockSize());
        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }
}
