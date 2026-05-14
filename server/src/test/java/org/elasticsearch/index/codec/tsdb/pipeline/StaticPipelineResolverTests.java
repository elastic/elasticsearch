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

public class StaticPipelineResolverTests extends ESTestCase {

    public void testResolvesBaselinePipelineShape() {
        final StaticPipelineResolver resolver = new StaticPipelineResolver();
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), randomFieldName()));

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(3, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testPropagatesBlockSizeFromContext() {
        final StaticPipelineResolver resolver = new StaticPipelineResolver();
        final int blockSize = randomBlockSize();

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomFieldName()));

        assertEquals(blockSize, config.blockSize());
    }

    public void testIgnoresFieldName() {
        final StaticPipelineResolver resolver = new StaticPipelineResolver();
        final int blockSize = randomBlockSize();

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, "@timestamp"));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, "metric.value"));

        assertEquals(first, second);
    }

    public void testRepeatedResolveReturnsEqualConfigs() {
        final StaticPipelineResolver resolver = new StaticPipelineResolver();
        final FieldContext context = new FieldContext(randomBlockSize(), randomFieldName());

        assertEquals(resolver.resolve(context), resolver.resolve(context));
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(4, 10);
    }

    private static String randomFieldName() {
        return randomAlphaOfLengthBetween(1, 16);
    }
}
