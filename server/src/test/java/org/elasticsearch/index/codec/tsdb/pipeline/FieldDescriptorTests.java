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
import org.apache.lucene.util.packed.DirectMonotonicReader;
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
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, 100, new long[] { 0L });

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
        final long totalValueCount = 1000;
        final long[] blockOffsets = { 0L, 100L, 200L, 300L, 400L, 500L, 600L, 700L };

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, totalValueCount, blockOffsets);

            try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
                final FieldDescriptor header = FieldDescriptor.read(meta);

                assertEquals(pipeline, header.pipeline());
                assertEquals(totalValueCount, header.totalValueCount());
                assertEquals(blockOffsets.length, header.blockCount());
                assertEquals(blockSize, header.blockSize());
            }
        }
    }

    public void testBlockOffsetsRandomAccess() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final long totalValueCount = 512;
        final long[] blockOffsets = { 0L, 50L, 120L, 180L };

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, totalValueCount, blockOffsets);
            final FieldDescriptor header = readFieldDescriptor(dir);

            try (IndexInput data = dir.openInput("test.data", IOContext.DEFAULT)) {
                final DirectMonotonicReader offsetsReader = header.getOffsetsReader(data);
                for (int i = 0; i < blockOffsets.length; i++) {
                    assertEquals(blockOffsets[i], offsetsReader.get(i));
                }
            }
        }
    }

    public void testEmptyField() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, 0, new long[0]);
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(0, header.totalValueCount());
            assertEquals(0, header.blockCount());
            assertEquals(0, header.lastBlockValueCount());
        }
    }

    public void testLastBlockValueCountFullBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final long totalValueCount = blockSize * 4L;
        final long[] blockOffsets = { 0L, 100L, 200L, 300L };

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, totalValueCount, blockOffsets);
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(blockSize, header.lastBlockValueCount());
        }
    }

    public void testLastBlockValueCountPartialBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final int partialSize = randomIntBetween(1, blockSize - 1);
        final long totalValueCount = blockSize * 3L + partialSize;
        final long[] blockOffsets = { 0L, 100L, 200L, 300L };

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, totalValueCount, blockOffsets);
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(partialSize, header.lastBlockValueCount());
        }
    }

    public void testLargeNumberOfBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);
        final int numBlocks = 1000;
        final long totalValueCount = (long) numBlocks * blockSize;
        final long[] blockOffsets = new long[numBlocks];

        long offset = 0;
        for (int i = 0; i < numBlocks; i++) {
            blockOffsets[i] = offset;
            offset += 50 + randomIntBetween(0, 100);
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, totalValueCount, blockOffsets);
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(numBlocks, header.blockCount());

            try (IndexInput data = dir.openInput("test.data", IOContext.DEFAULT)) {
                final DirectMonotonicReader offsetsReader = header.getOffsetsReader(data);
                for (int i = 0; i < numBlocks; i++) {
                    assertEquals(blockOffsets[i], offsetsReader.get(i));
                }
            }
        }
    }

    public void testDeltaOfDeltaPipeline() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.DELTA.id, StageId.OFFSET.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, 256, new long[] { 0L, 30L });
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(4, header.pipeline().pipelineLength());
            assertEquals(StageId.DELTA.id, header.pipeline().stageIdAt(0));
            assertEquals(StageId.DELTA.id, header.pipeline().stageIdAt(1));
        }
    }

    public void testToString() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.DELTA.id, StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, 1000, new long[] { 0L, 100L });
            final FieldDescriptor header = readFieldDescriptor(dir);

            final String str = header.toString();
            assertThat(str, containsString("totalValueCount=1000"));
            assertThat(str, containsString("blockCount=2"));
        }
    }

    public void testSingleBlock() throws IOException {
        final int blockSize = randomBlockSize();
        final byte[] stageIds = { StageId.BIT_PACK.id };
        final PipelineDescriptor pipeline = new PipelineDescriptor(stageIds, blockSize);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeFieldDescriptor(dir, pipeline, 100, new long[] { 0L });
            final FieldDescriptor header = readFieldDescriptor(dir);

            assertEquals(1, header.blockCount());
            assertEquals(100, header.lastBlockValueCount());

            try (IndexInput data = dir.openInput("test.data", IOContext.DEFAULT)) {
                final DirectMonotonicReader offsetsReader = header.getOffsetsReader(data);
                assertEquals(0L, offsetsReader.get(0));
            }
        }
    }

    private void writeFieldDescriptor(
        final Directory dir,
        final PipelineDescriptor pipeline,
        long totalValueCount,
        final long[] blockOffsets
    ) throws IOException {
        try (
            IndexOutput meta = dir.createOutput("test.meta", IOContext.DEFAULT);
            IndexOutput data = dir.createOutput("test.data", IOContext.DEFAULT)
        ) {
            FieldDescriptor.write(meta, data, pipeline, totalValueCount, blockOffsets);
        }
    }

    private FieldDescriptor readFieldDescriptor(final Directory dir) throws IOException {
        try (IndexInput meta = dir.openInput("test.meta", IOContext.DEFAULT)) {
            return FieldDescriptor.read(meta);
        }
    }
}
