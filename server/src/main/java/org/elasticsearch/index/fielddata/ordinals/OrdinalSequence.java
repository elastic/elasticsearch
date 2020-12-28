/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Stores sequences of non-native {@code long}s to disk for easy reading later.
 * 
 * Write in a single thread like this:
 * <pre><code>
 *    OrdinalSequence.GroupReader gReader;
 *    OrdinalSequence.ReaderProvider readerProvider1;
 *    OrdinalSequence.ReaderProvider readerProvider;
 *    try (OrdinalSequence.GroupWriter gWriter = OrdinalSequence.GroupWriter.tmpFile("test", "test", directory)) {
 *        OrdinalSequence.Writer writer1 = gWriter.nonNegativeWriter(0, 100);
 *        OrdinalSequence.Writer writer2 = gWriter.positiveDeltaWriter(100);
 *        for (int i = 0; i &lt; 100; i++) {
 *            writer1.add(i, whatever);
 *            writer2.add(i, whatever);
 *        }
 *        gReader = gWriter.finish();
 *        readerProvider1 = writer1.readerProvider();
 *        readerProvider2 = writer2.readerProvider();
 *    }
 * </code></pre>
 * 
 * Read in any number of threads like this:
 * <pre><code>
 *    RandomAccessInput in = gReader.input();
 *    LongUnaryOperator reader1 = readerProvider1.get(in);
 *    LongUnaryOperator reader2 = readerProvider2.get(in);
 *    int sum = 0;
 *    for (int i = 0; i &lt; 100; i++) {
 *        sum += reader1.get(i) + reader2.get(i);
 *    }
 *    return sum;
 * </code></pre>
 */
public class OrdinalSequence {
    /**
     * Number of bits of address in each block written to disk. Higher numbers
     * can require larger buffers and more memory to handle the readers but will
     * compress the map better. 
     */
    private static final int BLOCK_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

    /**
     * Writes groups of sequences. The package private methods return
     * {@link Writer}s that interleave their output into the single
     * {@link IndexOutput} managed by this {@link GroupWriter}.
     */
    abstract static class GroupWriter implements Closeable {
        private final List<Writer> unfinished = new ArrayList<>();
        private IndexOutput output;
        private boolean finished;

        protected abstract IndexOutput buildOutput() throws IOException;

        protected abstract GroupReader buildReader(IndexOutput output) throws IOException;

        /**
         * Writer for sequences of non-negative longs.
         */
        Writer nonNegativeWriter(long minValues, long maxValues) {
            return track(new Writer((index, value) -> value, (index, enc) -> enc, directBlockWriterBuilder(minValues, maxValues)));
        }

        /**
         * Writer for sequences that have values larger than or equal to their
         * index, meaning that {@code f(i) >= i}. Values are packed more efficiently
         * {@code f(i) - i} is near {@code 0}. That difference, <strong>must</strong>
         * grow, so {@code f(i) - i >= f(i - 1) - i}.
         */
        Writer positiveDeltaWriter(long valueCount) {
            return track(
                new Writer(
                    (index, value) -> value - index,
                    (index, enc) -> enc + index,
                    leadingZeros -> new MonotonicDelegateWriter(output(), valueCount - leadingZeros)
                )
            );
        }

        /**
         * Writer for sequences that have values smaller than or equal to their
         * index, meaning that {@code f(i) <= i}. Values are packed more efficiently
         * {@code i - f(i)} is near {@code 0}.
         */
        Writer negativeDeltaWriter(long minValues, long maxValues) {
            return track(
                new Writer((index, value) -> index - value, (index, enc) -> index - enc, directBlockWriterBuilder(minValues, maxValues))
            );
        }

        private DelegateWriter.Builder directBlockWriterBuilder(long minValues, long maxValues) {
            return leadingZeros -> {
                long realMin = Math.max(1, minValues - leadingZeros);
                long realMax = Math.max(realMin, maxValues - leadingZeros);
                return new DirectBlockWriter(output(), realMin, realMax);
            };
        }

        private Writer track(Writer writer) {
            unfinished.add(writer);
            return writer;
        }

        private IndexOutput output() throws IOException {
            if (output == null) {
                output = buildOutput();
            }
            return output;
        }

        GroupReader finish() throws IOException {
            if (finished) {
                throw new IllegalStateException("can only finish one time");
            }
            finished = true;
            for (Writer w : unfinished) {
                w.finish();
            }
            if (output == null) {
                return GroupReader.EMPTY;
            }
            output.close();
            return buildReader(output);
        }

        @Override
        public void close() throws IOException {
            if (output == null) {
                // Nothing to clean up
                return;
            }
            try {
                output.close();
            } finally {
                if (finished) {
                    // Finishing fetches a reader which now has owner ship of the files 
                    return;
                }
                /*
                 * We're closing without having finished building the output so
                 * something has very likely gone wrong. Delete the file we were
                 * working on.  
                 */
                buildReader(output).close();
            }
        }

        static GroupWriter tmpFile(String prefix, String suffix, Directory directory) {
            return new GroupWriter() {
                @Override
                protected IndexOutput buildOutput() throws IOException {
                    return directory.createTempOutput(prefix, suffix, IOContext.DEFAULT);
                }

                @Override
                protected GroupReader buildReader(IndexOutput out) throws IOException {
                    return GroupReader.directory(directory, out.getName());
                }
            };
        }
    }

    /**
     * Handle to read groups of sequences written at one with {@link GroupWriter}.
     * {@link GroupReader#close} it to delete the file from disk.
     */
    interface GroupReader extends Closeable {
        /**
         * Grab a new {@link RandomAccessInput} to pass to {@link ReaderProvider#get}.
         * This method is thread safe but the {@linkplain RandomAccessInput} it
         * returns isn't. Call this once to "open".
         */
        RandomAccessInput input() throws IOException;

        /**
         * How many bytes the sequences take up on disk.
         */
        long diskBytesUsed() throws IOException;

        GroupReader EMPTY = new GroupReader() {
            @Override
            public RandomAccessInput input() throws IOException {
                return null;
            }

            @Override
            public long diskBytesUsed() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {}
        };

        static GroupReader directory(Directory directory, String name) throws IOException {
            IndexInput input = directory.openInput(name, IOContext.READ);
            return new GroupReader() {
                @Override
                public RandomAccessInput input() throws IOException {
                    /*
                     * Slicing gives us the type we need to pass off to
                     * ReaderProvider and gets a thread-local copy
                     * of the reader.
                     */
                    return input.randomAccessSlice(0, input.length());
                }

                @Override
                public long diskBytesUsed() throws IOException {
                    return directory.fileLength(name);
                }

                @Override
                public void close() throws IOException {
                    try {
                        input.close();
                    } finally {
                        directory.deleteFile(name);
                    }
                }
            };
        }
    }

    static class Writer {
        private final LongBinaryOperator encode;
        private final LongBinaryOperator decode;
        private final DelegateWriter.Builder createWriter;

        private DelegateWriter writer;
        private long next;
        private long leadingZeros = -1;
        private long lastEncoded;

        private Writer(LongBinaryOperator encode, LongBinaryOperator decode, DelegateWriter.Builder createWriter) {
            this.encode = encode;
            this.decode = decode;
            this.createWriter = createWriter;
        }

        void add(long index, long value) throws IOException {
            if (index < next) {
                throw new IllegalArgumentException("Already wrote [" + index + "]");
            }
            lastEncoded = encode.applyAsLong(index, value);
            if (writer == null) {
                if (lastEncoded == 0) {
                    // Leading identity values are just offset.
                    return;
                }
                next = leadingZeros = index;
                writer = createWriter.build(leadingZeros);
            }
            /*
             * Friendly callers always increment the index by one each time
             * but not everyone is friendly. The unfriendly folks get sequences
             * of the same delta. They *should* never read it anyway but we don't
             * have any way of encoding gaps.
             */
            while (next < index) {
                writer.add(lastEncoded);
                next++;
            }
            writer.add(lastEncoded);
            next++;
        }

        private void finish() throws IOException {
            if (writer == null) {
                return;
            }
            writer.finish(leadingZeros, next, lastEncoded);
        }

        ReaderProvider readerProvider() throws IOException {
            if (writer == null) {
                return Identity.readerProvider(decode);
            }

            // OrdinalMap has a way of detecting if the delta encoding actually saves space. We skip it for now.
            return ReadFromDisk.provider(decode, writer.readerProvider(), leadingZeros);
        }
    }

    interface ReaderProvider extends Accountable {
        LongUnaryOperator get(RandomAccessInput input) throws IOException;
    }

    private interface DelegateReaderProvider extends Accountable {
        LongUnaryOperator get(RandomAccessInput in) throws IOException;

        DelegateReaderProvider ZEROS = new DelegateReaderProvider() {
            @Override
            public LongUnaryOperator get(RandomAccessInput in) throws IOException {
                return index -> 0;
            }

            @Override
            public long ramBytesUsed() {
                return 0; // Shared instance
            }
        };
    }

    private interface DelegateWriter {
        interface Builder {
            DelegateWriter build(long leadingZeros) throws IOException;
        }

        void add(long v) throws IOException;

        void finish(long leadingZeros, long next, long previous) throws IOException;

        DelegateReaderProvider readerProvider() throws IOException;
    }

    /**
     * A sequence that where {@code f(i) == i}.
     */
    private static class Identity implements LongUnaryOperator {
        static final ReaderProvider readerProvider(LongBinaryOperator decode) {
            Identity identity = new Identity(decode);
            return new ReaderProvider() {
                @Override
                public Identity get(RandomAccessInput input) throws IOException {
                    return identity;
                }

                @Override
                public long ramBytesUsed() {
                    return RamUsageEstimator.alignObjectSize(
                        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                    );
                }
            };
        }

        private final LongBinaryOperator decode;

        Identity(LongBinaryOperator decode) {
            this.decode = decode;
        }

        @Override
        public long applyAsLong(long globalOrd) {
            return decode.applyAsLong(globalOrd, 0);
        }
    }

    private static class ReadFromDisk implements LongUnaryOperator {
        static OrdinalSequence.ReaderProvider provider(
            LongBinaryOperator decode,
            DelegateReaderProvider delegateReaderProvider,
            long firstNonZeroEncoded
        ) throws IOException {
            return new ReaderProvider() {
                @Override
                public ReadFromDisk get(RandomAccessInput input) throws IOException {
                    return new ReadFromDisk(decode, delegateReaderProvider.get(input), firstNonZeroEncoded);
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + delegateReaderProvider.ramBytesUsed();
                    size += Long.BYTES;
                    return RamUsageEstimator.alignObjectSize(size);
                }
            };
        }

        private final LongBinaryOperator decode;
        private final LongUnaryOperator reader;
        private final long firstNonZeroEncoded;

        ReadFromDisk(LongBinaryOperator decode, LongUnaryOperator reader, long firstNonZeroEncoded) throws IOException {
            this.decode = decode;
            this.reader = reader;
            this.firstNonZeroEncoded = firstNonZeroEncoded;
        }

        @Override
        public long applyAsLong(long index) {
            if (index < firstNonZeroEncoded) {
                return decode.applyAsLong(index, 0);
            }
            return decode.applyAsLong(index, reader.applyAsLong(index - firstNonZeroEncoded));
        }
    }

    /**
     * Write sequences of integers. This writer splits data into blocks and
     * writes each block with a {@link DirectWriter} appropriate for the
     * maximum value. Think of it like a simplistic {@link DirectMonotonicWriter}.
     */
    private static class DirectBlockWriter implements DelegateWriter {
        private final IndexOutput out;
        private final int maxNumBlocks;
        private final int maxBlockSize;

        private long[] buffer;
        private int bufferIndex;
        private long bufferMax;

        private DelegateReaderProvider[] readerBuilders;
        private int currentBlock;
        private boolean finished;

        DirectBlockWriter(IndexOutput out, long minValues, long maxValues) {
            if (minValues > maxValues) {
                throw new IllegalArgumentException("minValues [" + minValues + "] must be < maxValues [" + maxValues + "]");
            }
            if (minValues < 0) {
                throw new IllegalArgumentException("minValues can't be negative but was [" + minValues + "]");
            }
            long minNumBlocks = maxValues == 0 ? 0 : ((minValues - 1) >>> BLOCK_SHIFT) + 1;
            long maxNumBlocks = maxValues == 0 ? 0 : ((maxValues - 1) >>> BLOCK_SHIFT) + 1;
            if (maxNumBlocks > ArrayUtil.MAX_ARRAY_LENGTH) {
                throw new IllegalArgumentException("too many blocks [" + maxNumBlocks + "]");
            }
            this.out = out;
            this.maxNumBlocks = (int) maxNumBlocks;
            this.maxBlockSize = (int) Math.min(maxValues, BLOCK_SIZE);
            this.buffer = new long[(int) Math.min(minValues, BLOCK_SIZE)];
            this.readerBuilders = new DelegateReaderProvider[(int) minNumBlocks];
        }

        private void flush() throws IOException {
            assert bufferIndex > 0;

            if (currentBlock == readerBuilders.length) {
                readerBuilders = ArrayUtil.growExact(
                    readerBuilders,
                    Math.min(maxNumBlocks, ArrayUtil.oversize(currentBlock + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF))
                );
            }

            if (bufferMax == 0) {
                readerBuilders[currentBlock++] = DelegateReaderProvider.ZEROS;
                return;
            }
            long blockStart = out.getFilePointer();
            int bitsPerValue = DirectWriter.bitsRequired(bufferMax);
            DirectWriter writer = DirectWriter.getInstance(out, bufferIndex, bitsPerValue);
            for (int i = 0; i < bufferIndex; ++i) {
                writer.add(buffer[i]);
            }
            writer.finish();
            readerBuilders[currentBlock++] = new DelegateReaderProvider() {
                @Override
                public LongUnaryOperator get(RandomAccessInput in) throws IOException {
                    LongValues reader = DirectReader.getInstance(in, bitsPerValue, blockStart);
                    return index -> {
                        if (index > BLOCK_SIZE) {
                            throw new IllegalArgumentException("reading wrong block");
                        }
                        return reader.get(index);
                    };
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += Long.BYTES;
                    size += Integer.BYTES;
                    return RamUsageEstimator.alignObjectSize(size);
                }
            };

            bufferMax = 0;
            bufferIndex = 0;
        }

        @Override
        public void add(long v) throws IOException {
            if (v < 0) {
                throw new IllegalArgumentException("Can only write 0 and positive numbers but tried to write [" + v + "]");
            }
            if (bufferIndex == buffer.length) {
                if (buffer.length < maxBlockSize) {
                    buffer = ArrayUtil.growExact(buffer, Math.min(maxBlockSize, ArrayUtil.oversize(bufferIndex + 1, Long.BYTES)));
                } else {
                    flush();
                }
            }
            buffer[bufferIndex++] = v;
            bufferMax = Math.max(bufferMax, v);
        }

        /**
         * This must be called exactly once after all values have been {@link #add(long) added}.
         */
        @Override
        public void finish(long leadingZeros, long next, long previous) throws IOException {
            if (finished) {
                throw new IllegalStateException("#finish has been called already");
            }
            if (bufferIndex > 0) {
                flush();
            }
            finished = true;
        }

        @Override
        public DelegateReaderProvider readerProvider() throws IOException {
            if (finished == false) {
                throw new IllegalStateException("#finish has not been called");
            }
            return new DelegateReaderProvider() {
                @Override
                public LongUnaryOperator get(RandomAccessInput in) throws IOException {
                    LongUnaryOperator[] readers = new LongUnaryOperator[readerBuilders.length];
                    return new LongUnaryOperator() {
                        @Override
                        public long applyAsLong(long index) {
                            int block = (int) (index >>> BLOCK_SHIFT);
                            long blockIndex = index & ((1 << BLOCK_SHIFT) - 1);
                            return reader(block).applyAsLong(blockIndex);
                        }

                        private LongUnaryOperator reader(int block) {
                            if (readers[block] == null) {
                                try {
                                    readers[block] = readerBuilders[block].get(in);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            return readers[block];
                        }
                    };
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.sizeOf(readerBuilders);
                    return RamUsageEstimator.alignObjectSize(size);
                }
            };
        }
    }

    private static class MonotonicDelegateWriter implements DelegateWriter {
        private final long valueCount;
        private final ByteBuffersDataOutput meta;
        private final IndexOutput metaOut, dataOut;
        private final DirectMonotonicWriter writer;

        MonotonicDelegateWriter(IndexOutput dataOut, long valueCount) throws IOException {
            this.valueCount = valueCount;
            meta = new ByteBuffersDataOutput();
            metaOut = new ByteBuffersIndexOutput(meta, "temp", "meta");
            this.dataOut = dataOut;
            writer = DirectMonotonicWriter.getInstance(metaOut, dataOut, valueCount, BLOCK_SHIFT);
        }

        @Override
        public void add(long v) throws IOException {
            writer.add(v);
        }

        @Override
        public void finish(long leadingZeros, long next, long previous) throws IOException {
            /*
             * Fill in any missing values to keep the writer happy. Good callers
             * won't have any so we're not wasting space for good callers. Bad
             * callers probably won't have *many* so even then we're not to bad. 
             */
            next -= leadingZeros;
            while (next < valueCount) {
                previous++;
                writer.add(previous);
                next++;
            }
            writer.finish();
        }

        @Override
        public DelegateReaderProvider readerProvider() throws IOException {
            IOUtils.close(metaOut, dataOut);
            DirectMonotonicReader.Meta meta;
            try (IndexInput metaIn = new ByteBuffersIndexInput(new ByteBuffersDataInput(this.meta.toBufferList()), "temp")) {
                meta = DirectMonotonicReader.loadMeta(metaIn, valueCount, BLOCK_SHIFT);
            }
            // OrdinalMap can detect if the delta encoding is larger than just using the direct encoding. We skip that for now.
            return new DelegateReaderProvider() {
                @Override
                public LongUnaryOperator get(RandomAccessInput in) throws IOException {
                    return DirectMonotonicReader.getInstance(meta, in)::get;
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + meta.ramBytesUsed();
                    return RamUsageEstimator.alignObjectSize(size);
                }
            };
        }
    }
}
