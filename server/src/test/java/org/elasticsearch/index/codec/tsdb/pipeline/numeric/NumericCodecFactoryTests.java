/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.test.ESTestCase;

public class NumericCodecFactoryTests extends ESTestCase {

    public void testCreateEncoderProducesDescriptorMatchingConfig() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final PipelineDescriptor descriptor = encoder.descriptor();

        assertEquals(blockSize, encoder.blockSize());
        assertEquals(PipelineDescriptor.DataType.LONG, descriptor.dataType());
        assertEquals(config.specs().size(), descriptor.pipelineLength());
        for (int i = 0; i < config.specs().size(); i++) {
            assertEquals(config.specs().get(i).stageId().id, descriptor.stageIdAt(i));
        }
    }

    public void testCreateEncoderProducesIndependentBlockEncoders() {
        final PipelineConfig config = PipelineConfig.forLongs(randomBlockSize()).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        assertNotSame(encoder.newBlockEncoder(), encoder.newBlockEncoder());
    }

    public void testCreateDecoderFromDescriptorMatchesBlockSize() {
        final int blockSize = randomBlockSize();
        final PipelineConfig config = PipelineConfig.forDoubles(blockSize).alpDoubleStage().delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());

        assertNotNull(decoder);
        assertEquals(blockSize, decoder.blockSize());
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 12);
    }
}
