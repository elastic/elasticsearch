/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.columnar.substrate.MonotonicWriter;
import org.elasticsearch.columnar.substrate.StagedBytes;

import java.io.Closeable;
import java.io.IOException;

/**
 * A sequence of longs stored as encoded blocks, with an offset for each block so one is reached without
 * reading those before it.
 *
 * <p>This is what a column of numbers is made of, and it is all a caller wants that has no documents to
 * speak of — a count a document, a length a value. {@link NumericColumnWriter} adds to it the parts that
 * belong to documents: which of them have a value, where each one's values begin, and the skip index.
 */
public final class LongBlocks {

    private LongBlocks() {}

    /** Where a sequence landed and how to decode it, which is what a reader needs and no more. */
    public record Metadata(
        long numValues,
        int blockSize,
        byte blockBytesCodecId,
        byte terminalId,
        byte[] transformIds,
        long valuesOffset,
        MonotonicWriter.Table blockOffsets
    ) {
        /** How many blocks the sequence occupies. */
        public long numBlocks() {
            return (numValues + blockSize - 1) / blockSize;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeVLong(numValues);
            out.writeVInt(blockSize);
            out.writeByte(blockBytesCodecId);
            out.writeByte(terminalId);
            out.writeVInt(transformIds.length);
            out.writeBytes(transformIds, 0, transformIds.length);
            out.writeVLong(valuesOffset);
            out.writeVLong(blockOffsets.dataOffset());
            out.writeVLong(blockOffsets.dataLength());
            out.writeVInt(blockOffsets.meta().length);
            out.writeBytes(blockOffsets.meta(), 0, blockOffsets.meta().length);
        }

        public static Metadata readFrom(DataInput in) throws IOException {
            final long numValues = in.readVLong();
            final int blockSize = in.readVInt();
            final byte codecId = in.readByte();
            final byte terminalId = in.readByte();
            final byte[] transformIds = new byte[in.readVInt()];
            in.readBytes(transformIds, 0, transformIds.length);
            final long valuesOffset = in.readVLong();
            final long offsetsData = in.readVLong();
            final long offsetsLength = in.readVLong();
            final byte[] offsetsMeta = new byte[in.readVInt()];
            in.readBytes(offsetsMeta, 0, offsetsMeta.length);
            return new Metadata(
                numValues,
                blockSize,
                codecId,
                terminalId,
                transformIds,
                valuesOffset,
                new MonotonicWriter.Table(offsetsData, offsetsLength, offsetsMeta)
            );
        }
    }

    /**
     * Takes values one at a time and writes them as blocks.
     *
     * <p>A caller that owns the column output writes into it directly. One that is still writing something
     * else to the column cannot, so it stages the blocks instead and they are copied in on {@link #finish}.
     * Either way {@link #finish} answers where they ended up.
     *
     * <p>{@code numValues} is how many values the caller will add, exactly: the table of block offsets is
     * sized from it and rejects a different count.
     */
    public static final class Writer implements Closeable {

        private final NumericPipeline pipeline;
        private final BlockBytesCodec blockBytesCodec;
        private final int blockSize;

        /** Null when the blocks go straight into the column, in which case nothing is staged. */
        private final StagedBytes staged;
        private final IndexOutput out;
        private final long directOffset;
        private final MonotonicWriter blockOffsets;

        private final NumericBlockEncoder encoder;
        private final long[] buffer;
        private final int[] blockValueCount = new int[1];
        private final BlockBytesCodec.BlockEncoder blockEncoder;

        private int inBlock;
        private long added;
        private boolean finished;

        /**
         * Blocks written straight into {@code data}, for a caller that owns it until they are done. Nothing
         * is copied and nothing is staged.
         */
        public static Writer into(
            NumericPipeline pipeline,
            BlockBytesCodec codec,
            long numValues,
            Directory directory,
            IOContext context,
            String prefix,
            IndexOutput data
        ) throws IOException {
            return new Writer(pipeline, codec, numValues, directory, context, prefix, null, data);
        }

        /**
         * Blocks staged in a temporary file and copied into the column on {@link #finish}, for a caller
         * that is writing something else to it meanwhile. {@code suffix} names the file after what it
         * holds, so one left behind says which caller left it.
         */
        public static Writer staged(
            NumericPipeline pipeline,
            BlockBytesCodec codec,
            long numValues,
            Directory directory,
            IOContext context,
            String prefix,
            String suffix
        ) throws IOException {
            StagedBytes bytes = null;
            try {
                bytes = new StagedBytes(directory, context, prefix, suffix);
                return new Writer(pipeline, codec, numValues, directory, context, prefix, bytes, bytes.output());
            } catch (Throwable t) {
                IOUtils.closeWhileHandlingException(bytes);
                throw t;
            }
        }

        private Writer(
            NumericPipeline pipeline,
            BlockBytesCodec blockBytesCodec,
            long numValues,
            Directory directory,
            IOContext context,
            String prefix,
            StagedBytes staged,
            IndexOutput out
        ) throws IOException {
            this.pipeline = pipeline;
            this.blockBytesCodec = blockBytesCodec;
            this.blockSize = pipeline.blockSize();
            this.buffer = new long[blockSize];
            this.encoder = new NumericBlockEncoder(pipeline, blockSize);
            // One reusable closure over the buffer, so no lambda is allocated per block flush.
            this.blockEncoder = o -> encoder.encode(buffer, blockValueCount[0], o);
            this.staged = staged;
            this.out = out;
            // A direct writer shares the column output, so a block offset counts from where it began.
            this.directOffset = staged == null ? out.getFilePointer() : 0;
            this.blockOffsets = new MonotonicWriter(directory, context, prefix, (numValues + blockSize - 1) / blockSize + 1L);
        }

        /** Adds the next value of the sequence. */
        public void add(long value) throws IOException {
            if (inBlock == 0) {
                blockOffsets.add(written());
            }
            buffer[inBlock++] = value;
            added++;
            if (inBlock == blockSize) {
                flush(blockSize);
            }
        }

        /** Copies the blocks into {@code data}, writes where each begins into {@code navigation}, and answers where they landed. */
        public Metadata finish(IndexOutput data, IndexOutput navigation) throws IOException {
            if (inBlock > 0) {
                // The last block holds fewer than blockSize values; the encoder is told the real count and
                // never sees padding, so each stage fits only the real data.
                flush(inBlock);
            }
            blockOffsets.add(written());
            finished = true;
            final long valuesOffset = staged == null ? directOffset : staged.copyInto(data);
            final MonotonicWriter.Table offsets = blockOffsets.finish(navigation);
            return new Metadata(
                added,
                blockSize,
                blockBytesCodec.id(),
                pipeline.terminalId(),
                pipeline.transformIds(),
                valuesOffset,
                offsets
            );
        }

        /** Block bytes written so far, which is what a block offset is relative to. */
        private long written() {
            return out.getFilePointer() - directOffset;
        }

        private void flush(int count) throws IOException {
            blockValueCount[0] = count;
            blockBytesCodec.write(blockEncoder, out);
            inBlock = 0;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(staged, blockOffsets);
        }
    }

    /** Decodes one block at a time out of what {@link Writer} wrote, holding only the block it last read. */
    public static final class Reader {

        private final Metadata meta;
        private final IndexInput data;
        private final BlockBytesCodec blockBytesCodec;
        private final NumericBlockEncoder encoder;
        private final org.apache.lucene.util.LongValues blockOffsets;
        private final long[] blockBuffer;

        private long cachedBlock = -1;

        public Reader(Metadata meta, IndexInput data, IndexInput navigation) throws IOException {
            this.meta = meta;
            this.data = data.clone();
            this.blockBytesCodec = BlockBytesCodec.forId(meta.blockBytesCodecId());
            this.encoder = new NumericBlockEncoder(
                NumericPipeline.Registry.rebuild(meta.terminalId(), meta.transformIds(), meta.blockSize()),
                meta.blockSize()
            );
            this.blockBuffer = new long[meta.blockSize()];
            this.blockOffsets = MonotonicReader.open(
                navigation,
                meta.blockOffsets().meta(),
                meta.numBlocks() + 1L,
                meta.blockOffsets().dataOffset(),
                meta.blockOffsets().dataLength()
            );
        }

        /** Values per block, which is how a caller turns a position into a block and an index in it. */
        public int blockSize() {
            return meta.blockSize();
        }

        /**
         * Decodes the block at {@code blockIndex} and returns the shared buffer, valid until the next call
         * that touches a different block.
         */
        public long[] block(long blockIndex) throws IOException {
            if (blockIndex == cachedBlock) {
                return blockBuffer;
            }
            final long start = meta.valuesOffset() + blockOffsets.get(blockIndex);
            final long end = meta.valuesOffset() + blockOffsets.get(blockIndex + 1);
            data.seek(start);
            final DataInput blockData = blockBytesCodec.read(data, (int) (end - start));
            // Full blocks hold blockSize values; the last block holds the remainder.
            final int valueCount = (int) Math.min(meta.blockSize(), meta.numValues() - blockIndex * meta.blockSize());
            encoder.decode(blockData, valueCount, blockBuffer);
            cachedBlock = blockIndex;
            return blockBuffer;
        }
    }
}
