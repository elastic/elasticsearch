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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FieldDescriptorWriterTests extends ESTestCase {

    public void testWritesDescriptorOnFirstEncode() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();

        final byte[] metaBuf = new byte[1024];
        final ByteArrayDataOutput metaOut = new ByteArrayDataOutput(metaBuf);
        final FieldDescriptorWriter writer = new FieldDescriptorWriter(metaOut, config, 128);

        final long[] values = new long[128];
        for (int i = 0; i < 128; i++) {
            values[i] = i;
        }

        final byte[] dataBuf = new byte[8192];
        final ByteArrayDataOutput dataOut = new ByteArrayDataOutput(dataBuf);

        writer.encode(values, 128, dataOut);

        assertTrue(metaOut.getPosition() > 0);
        assertNotNull(writer.descriptor());
    }

    public void testSubsequentEncodesDoNotRewriteDescriptor() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();

        final byte[] metaBuf = new byte[1024];
        final ByteArrayDataOutput metaOut = new ByteArrayDataOutput(metaBuf);
        final FieldDescriptorWriter writer = new FieldDescriptorWriter(metaOut, config, 128);

        final long[] values = new long[128];
        final byte[] dataBuf = new byte[8192];
        final ByteArrayDataOutput dataOut = new ByteArrayDataOutput(dataBuf);

        writer.encode(values, 128, dataOut);
        final int metaPosAfterFirst = metaOut.getPosition();

        writer.encode(values, 128, dataOut);
        assertEquals(metaPosAfterFirst, metaOut.getPosition());
    }

    public void testBlockSizeBeforeAndAfterEncode() throws IOException {
        final PipelineConfig config = PipelineConfig.forLongs(128).delta().offset().gcd().bitPack();

        final byte[] metaBuf = new byte[1024];
        final ByteArrayDataOutput metaOut = new ByteArrayDataOutput(metaBuf);
        final FieldDescriptorWriter writer = new FieldDescriptorWriter(metaOut, config, 128);

        assertEquals(128, writer.blockSize());

        final long[] values = new long[128];
        final byte[] dataBuf = new byte[8192];
        final ByteArrayDataOutput dataOut = new ByteArrayDataOutput(dataBuf);
        writer.encode(values, 128, dataOut);

        assertEquals(128, writer.blockSize());
    }

    public void testDefaultConfigGetsBlockSizeApplied() throws IOException {
        final byte[] metaBuf = new byte[1024];
        final ByteArrayDataOutput metaOut = new ByteArrayDataOutput(metaBuf);
        final FieldDescriptorWriter writer = new FieldDescriptorWriter(metaOut, PipelineConfig.defaultConfig(), 256);

        final long[] values = new long[256];
        final byte[] dataBuf = new byte[16384];
        final ByteArrayDataOutput dataOut = new ByteArrayDataOutput(dataBuf);
        writer.encode(values, 256, dataOut);

        assertNotNull(writer.descriptor());
        assertEquals(256, writer.blockSize());
    }
}
