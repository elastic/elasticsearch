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

public class StaticPipelineConfigResolverTests extends ESTestCase {

    public void testResolvesBaselinePipelineShape() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), randomFieldName()));

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(3, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testPropagatesBlockSizeFromContext() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomFieldName()));

        assertEquals(blockSize, config.blockSize());
    }

    public void testIgnoresFieldName() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, "@timestamp"));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, "metric.value"));

        assertEquals(first, second);
    }

    public void testRepeatedResolveReturnsEqualConfigs() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(randomBlockSize(), randomFieldName());

        assertEquals(resolver.resolve(context), resolver.resolve(context));
    }

    public void testCachedBlockSizesReturnSameInstance() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, randomFieldName()));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, randomFieldName()));
        assertSame(first, second);
    }

    public void testUncachedBlockSizeStillResolves() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = 1 << randomIntBetween(4, 6);

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomFieldName()));
        assertEquals(blockSize, config.blockSize());
        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(4, 10);
    }

    private static String randomFieldName() {
        return randomAlphaOfLengthBetween(1, 16);
    }
}
