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
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FieldDescriptorTests extends ESTestCase {

    private static int randomBlockSize() {
        return ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE << randomIntBetween(0, 7);
    }

    public void testFormatVersionIsWrittenAndRead() throws IOException {
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { StageId.DELTA.id, StageId.BIT_PACK.id }, randomBlockSize());

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

    public void testBasicRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.OFFSET.id, StageId.GCD.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT)) {
                FieldDescriptor.write(meta, pipeline);
            }

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final PipelineDescriptor desc = FieldDescriptor.read(meta);
                assertEquals(pipeline, desc);
            }
        }
    }

    public void testRoundTripWithDataType() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize, PipelineDescriptor.DataType.DOUBLE);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT)) {
                FieldDescriptor.write(meta, pipeline);
            }

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final PipelineDescriptor desc = FieldDescriptor.read(meta);
                assertEquals(pipeline, desc);
                assertEquals(PipelineDescriptor.DataType.DOUBLE, desc.dataType());
            }
        }
    }
}
