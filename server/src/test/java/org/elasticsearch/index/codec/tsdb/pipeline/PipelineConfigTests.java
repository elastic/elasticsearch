/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class PipelineConfigTests extends ESTestCase {

    public void testLongPipelineBuilds() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        assertNotNull(encoder);
        assertNotNull(encoder.newBlockEncoder());
        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
        assertNotNull(decoder.newBlockDecoder());
    }

    public void testDoublePipelineBuilds() throws IOException {
        final PipelineConfig config = PipelineConfig.forDoubles(128).alpDoubleStage().alpDouble();
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        assertNotNull(encoder);
        assertNotNull(encoder.newBlockEncoder());
        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
        assertNotNull(decoder.newBlockDecoder());
    }

    public void testBlockSizePreserved() throws IOException {
        for (int blockSize : new int[] { 128, 256, 512 }) {
            final PipelineConfig config = PipelineConfig.forLongs(blockSize).delta().bitPack();
            final NumericEncoder encoder = NumericEncoder.fromConfig(config);
            assertEquals(blockSize, encoder.blockSize());
            assertEquals(blockSize, encoder.descriptor().blockSize());
        }
    }

    public void testBuiltCodecHasDescriptor() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        final PipelineDescriptor desc = encoder.descriptor();
        assertNotNull(desc);
        assertTrue(desc.pipelineLength() > 0);
    }

    public void testEquality() {
        final PipelineConfig config1 = PipelineConfig.forLongs(128).delta().bitPack();
        final PipelineConfig config2 = PipelineConfig.forLongs(128).delta().bitPack();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testInequality() {
        final PipelineConfig config1 = PipelineConfig.forLongs(128).delta().bitPack();
        final PipelineConfig config2 = PipelineConfig.forLongs(128).offset().bitPack();

        assertNotEquals(config1, config2);
    }

    public void testDataTypePreserved() {
        final PipelineConfig longConfig = PipelineConfig.forLongs(128).delta().bitPack();
        final PipelineConfig doubleConfig = PipelineConfig.forDoubles(128).alpDoubleStage().alpDouble();

        assertEquals(PipelineConfig.DataType.LONG, longConfig.dataType());
        assertEquals(PipelineConfig.DataType.DOUBLE, doubleConfig.dataType());
    }

    public void testSpecsPreserved() {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().bitPack();

        assertEquals(3, config.specs().size());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testVariousLongPipelines() throws IOException {
        final PipelineConfig[] configs = {
            PipelineConfig.forLongs(128).delta().bitPack(),
            PipelineConfig.forLongs(128).offset().gcd().bitPack(),
            PipelineConfig.forLongs(128).rle().bitPack(),
            PipelineConfig.forLongs(128).delta().offset().gcd().bitPack(),
            PipelineConfig.forLongs(256).delta().offset().gcd().zstd() };

        for (final PipelineConfig config : configs) {
            final NumericEncoder encoder = NumericEncoder.fromConfig(config);
            assertNotNull(encoder);
            assertNotNull(encoder.newBlockEncoder());
            final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
            assertNotNull(decoder.newBlockDecoder());
        }
    }

    public void testVariousDoublePipelines() throws IOException {
        final PipelineConfig[] configs = {
            PipelineConfig.forDoubles(128).alpDoubleStage().alpDouble(),
            PipelineConfig.forDoubles(128).alpRdDoubleStage().alpRdDouble(),
            PipelineConfig.forDoubles(128).gorilla() };

        for (final PipelineConfig config : configs) {
            final NumericEncoder encoder = NumericEncoder.fromConfig(config);
            assertNotNull(encoder);
            assertNotNull(encoder.newBlockEncoder());
            final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
            assertNotNull(decoder.newBlockDecoder());
        }
    }

    public void testDescriptorMatchesBuiltPipeline() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        final PipelineDescriptor desc = encoder.descriptor();

        assertEquals(config.specs().size(), desc.pipelineLength());
        for (int i = 0; i < config.specs().size(); i++) {
            assertEquals(config.specs().get(i).stageId().id, desc.stageIdAt(i));
        }
    }
}
