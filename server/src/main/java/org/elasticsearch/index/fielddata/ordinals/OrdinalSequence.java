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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Stores sequences of non-native {@code long}s to disk for easy reading later.
 */
public class OrdinalSequence {
    interface ReaderProvider extends Accountable {
        LongUnaryOperator get(RandomAccessInput input) throws IOException;
    }

    static abstract class OutHelper implements Closeable {
        private final List<Writer> all = new ArrayList<>();
        private IndexOutput output;
        private InHelper input;

        protected abstract IndexOutput buildOutput() throws IOException;

        protected abstract InHelper buildInput(String name) throws IOException;

        private IndexOutput output() throws IOException {
            if (output == null) {
                output = buildOutput();
            }
            return output;
        }

        InHelper finish() throws IOException {
            if (input == null) {
                for (Writer w : all) {
                    w.finish();
                }
                if (output == null) {
                    input = InHelper.EMPTY;
                } else {
                    output.close();
                    input = buildInput(output.getName());
                }
            }
            return input;
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
                if (input != null) {
                    // Finishing up the input has transferred ownership of all resources to the input.
                    return;
                }
                /*
                 * We're closing without having finished building the output so
                 * something has very likely gone wrong. Delete the file we were
                 * working on.  
                 */
                buildInput(output.getName()).close();
            }
        }

        static OutHelper tmpFile(String prefix, String suffix, Directory directory) {
            return new OutHelper() {
                @Override
                protected IndexOutput buildOutput() throws IOException {
                    return directory.createTempOutput(prefix, suffix, IOContext.DEFAULT);
                }

                @Override
                protected InHelper buildInput(String name) throws IOException {
                    return InHelper.directory(directory, name);
                }
            };
        }
    }

    interface InHelper extends Closeable {
        RandomAccessInput input() throws IOException;

        long diskBytesUsed() throws IOException;

        static InHelper EMPTY = new InHelper() {
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

        static InHelper directory(Directory directory, String name) throws IOException {
            IndexInput input = directory.openInput(name, IOContext.READ);
            return new InHelper() {
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

    static Writer directWriter(OutHelper out, long valueCount, int bitsPerValue) {
        // NOCOMMIT should bitsPerValue be maxValue?
        return new Writer(
            (index, value) -> value,
            (index, enc) -> enc,
            vc -> new DirectDelegateWriter(out.output(), vc, bitsPerValue),
            valueCount
        );
    }

    /**
     * Writer for sequences that have values larger than or equal to their
     * index, meaning that {@code f(i) >= i}. Values are packed more efficiently
     * {@code f(i) - i} is near {@code 0}. That difference, <strong>must</strong>
     * grow, so {@code f(i) - i >= f(i - 1) - i}.
     */
    static Writer positiveDeltaWriter(OutHelper out, long valueCount) {
        return new Writer(
            (index, value) -> value - index,
            (index, enc) -> enc + index,
            vc -> new MonotonicDelegateWriter(out.output(), vc),
            valueCount
        );
    }

    /**
     * Writer for sequences that have values smaller than or equal to their
     * index, meaning that {@code f(i) <= i}. Values are packed more efficiently
     * {@code i - f(i)} is near {@code 0}.
     */
    static Writer negativeDeltaWriter(OutHelper out, long valueCount, long maxDelta) {
        return new Writer(
            (index, value) -> index - value,
            (index, enc) -> index - enc,
            vc -> new DirectDelegateWriter(out.output(), vc, DirectWriter.bitsRequired(maxDelta)),
            valueCount
        );
    }

    static class Writer {
        private final LongBinaryOperator encode;
        private final LongBinaryOperator decode;
        private final DelegateWriter.Builder createWriter;
        private final long valueCount;

        private DelegateWriter writer;
        private long next;
        private long firstNonZeroEncoded = -1;
        private long lastEncoded;
        private ReaderProvider readerProvider;

        private Writer(LongBinaryOperator encode, LongBinaryOperator decode, DelegateWriter.Builder createWriter, long valueCount) {
            this.encode = encode;
            this.decode = decode;
            this.createWriter = createWriter;
            this.valueCount = valueCount;
        }

        long add(long index, long value) throws IOException {
            if (readerProvider != null) {
                throw new UnsupportedOperationException("Can't add after calling readerProvider");
            }
            if (index < next) {
                throw new IllegalArgumentException("Already wrote [" + index + "]");
            }
            lastEncoded = encode.applyAsLong(index, value);
            if (writer == null) {
                if (lastEncoded == 0) {
                    // Leading identity values are just offset.
                    return 0;
                }
                writer = createWriter.build(valueCount - index);
                next = firstNonZeroEncoded = index;
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
            return lastEncoded;
        }

        private void finish() throws IOException {
            if (writer == null) {
                return;
            }
            /*
             * Fill in any missing values to keep the writer happy. Good callers
             * won't have any so we're not wasting space for good callers. Bad
             * callers probably won't have *many* so even then we're not to bad. 
             */
            while (next < valueCount) {
                lastEncoded++;
                writer.add(lastEncoded);
                next++;
            }
            writer.finish();
        }

        ReaderProvider readerProvider() throws IOException {
            if (readerProvider == null) {
                readerProvider = buildReaderProvider();
            }
            return readerProvider;
        }

        private ReaderProvider buildReaderProvider() throws IOException {
            if (writer == null) {
                return Identity.readerProvider(decode);
            }

            // OrdinalMap has a way of detecting if the delta encoding actually saves space. We skip it for now.
            return ReadFromDisk.provider(decode, writer.readerProvider(), firstNonZeroEncoded);
        }
    }

    private interface DelegateReaderProvider extends Accountable {
        LongUnaryOperator get(RandomAccessInput in) throws IOException;
    }

    private interface DelegateWriter {
        interface Builder {
            DelegateWriter build(long valueCount) throws IOException;
        }

        void add(long v) throws IOException;

        void finish() throws IOException;

        DelegateReaderProvider readerProvider() throws IOException;
    }

    private static class DirectDelegateWriter implements DelegateWriter {
        private final DirectWriter writer;
        private final int bitsPerValue;

        DirectDelegateWriter(IndexOutput out, long valueCount, int bitsPerValue) throws IOException {
            this.bitsPerValue = bitsPerValue;
            writer = DirectWriter.getInstance(out, valueCount, bitsPerValue);
        }

        @Override
        public void add(long v) throws IOException {
            writer.add(v);
        }

        @Override
        public void finish() throws IOException {
            writer.finish();
        }

        @Override
        public DelegateReaderProvider readerProvider() throws IOException {
            int bitsPerValue = this.bitsPerValue;
            return new DelegateReaderProvider() {
                @Override
                public LongUnaryOperator get(RandomAccessInput in) throws IOException {
                    return DirectReader.getInstance(in, bitsPerValue)::get;
                }

                @Override
                public long ramBytesUsed() {
                    return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
                }
            };
        }
    }

    private static class MonotonicDelegateWriter implements DelegateWriter {
        private static final int BLOCK_SHIFT = 16;

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
        public void finish() throws IOException {
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

}
