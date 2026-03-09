/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FieldDescriptorTests extends ESTestCase {

    private static final int BASE_BLOCK_SIZE = 128;

    private static int randomBlockSize() {
        return BASE_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testFormatVersionIsWrittenAndRead() throws IOException {
        final PipelineDescriptor pipeline = new PipelineDescriptor(
            randomStageIds(randomIntBetween(1, 5)),
            randomBlockSize(),
            randomFrom(PipelineDescriptor.DataType.values())
        );

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT)) {
                FieldDescriptor.write(meta, pipeline);
            }

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final int version = meta.readVInt();
                assertThat(version, equalTo(FieldDescriptor.CURRENT_FORMAT_VERSION));
            }
        }
    }

    private static byte[] randomStageIds(final int length) {
        final StageId[] allStages = StageId.values();
        final byte[] stageIds = new byte[length];
        for (int i = 0; i < length; i++) {
            stageIds[i] = allStages[randomIntBetween(0, allStages.length - 1)].id;
        }
        return stageIds;
    }

    public void testUnsupportedFormatVersionThrows() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT)) {
                meta.writeVInt(999);
            }

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final IOException e = expectThrows(IOException.class, () -> FieldDescriptor.read(meta));
                assertThat(e.getMessage(), containsString("Unsupported FieldDescriptor format version: 999"));
                assertThat(e.getMessage(), containsString("Maximum supported version is " + FieldDescriptor.CURRENT_FORMAT_VERSION));
            }
        }
    }

    public void testRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final PipelineDescriptor.DataType dataType = randomFrom(PipelineDescriptor.DataType.values());
        final byte[] stageIds = randomStageIds(randomIntBetween(1, PipelineDescriptor.MAX_PIPELINE_LENGTH));
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize, dataType);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT)) {
                FieldDescriptor.write(meta, pipeline);
            }

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final PipelineDescriptor desc = FieldDescriptor.read(meta);
                assertEquals(pipeline, desc);
                assertEquals(dataType, desc.dataType());
            }
        }
    }
}
