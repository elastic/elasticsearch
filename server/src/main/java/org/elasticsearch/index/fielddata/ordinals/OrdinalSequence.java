package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Stores sequences of non-native {@code long}s to disk for easy reading later.
 */
public class OrdinalSequence {
    interface ReaderProvider extends Accountable, Closeable {
        Reader get() throws IOException;

        long diskBytesUsed() throws IOException;
    }

    interface Reader extends LongUnaryOperator, Closeable {}

    interface IO {
        IndexOutput createOutput() throws IOException;

        IndexInput openInput(String name) throws IOException;

        long diskBytesUsed(String name) throws IOException;

        void delete(String name) throws IOException;
    }

    static Writer directWriter(IO io, long valueCount, int bitsPerValue) {
        // NOCOMMIT should bitsPerValue be maxValue?
        return new Writer((index, value) -> value, (index, enc) -> enc, vc -> new DirectDelegateWriter(io, vc, bitsPerValue), valueCount);
    }

    /**
     * Writer for sequences that have values larger than or equal to their
     * index, meaning that {@code f(i) >= i}. Values are packed more efficiently
     * {@code f(i) - i} is near {@code 0}. That difference, <strong>must</strong>
     * grow, so {@code f(i) - i >= f(i - 1) - i}.
     */
    static Writer positiveDeltaWriter(IO io, long valueCount) {
        return new Writer(
            (index, value) -> value - index,
            (index, enc) -> enc + index,
            vc -> new MonotonicDelegateWriter(io, vc),
            valueCount
        );
    }

    /**
     * Writer for sequences that have values smaller than or equal to their
     * index, meaning that {@code f(i) <= i}. Values are packed more efficiently
     * {@code i - f(i)} is near {@code 0}.
     */
    static Writer negativeDeltaWriter(IO io, long valueCount, long maxDelta) {
        return new Writer(
            (index, value) -> index - value,
            (index, enc) -> index - enc,
            vc -> new DirectDelegateWriter(io, vc, DirectWriter.bitsRequired(maxDelta)),
            valueCount
        );
    }

    private interface DelegateReaderProvider extends Accountable, Closeable {
        DelegateReader get() throws IOException;

        long diskBytesUsed() throws IOException;
    }

    private interface DelegateReader extends Closeable {
        long get(long index);
    }

    private interface DelegateWriter extends Closeable {
        interface Builder {
            DelegateWriter build(long valueCount) throws IOException;
        }

        void add(long v) throws IOException;

        DelegateReaderProvider readerProvider() throws IOException;
    }

    static class Writer implements Closeable {
        private final LongBinaryOperator encode;
        private final LongBinaryOperator decode;
        private final DelegateWriter.Builder createWriter;
        private final long valueCount;

        private DelegateWriter writer;
        private long next;
        private long firstNonZeroEncoded = -1;
        private long lastEncoded;

        private Writer(LongBinaryOperator encode, LongBinaryOperator decode, DelegateWriter.Builder createWriter, long valueCount) {
            this.encode = encode;
            this.decode = decode;
            this.createWriter = createWriter;
            this.valueCount = valueCount;
        }

        long add(long index, long value) throws IOException {
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

        OrdinalSequence.ReaderProvider readerProvider() throws IOException {
            // NOCOMMIT make sure 0 byte files are not created.
            if (writer == null) {
                return Identity.readerProvider(decode);
            }
            /*
             * Fill in any missing values to keep the writer happy. Good callers
             * won't have any so we're not wasting space for good callers. Bad
             * callers probably won't have *many* so ever then we're not to bad. 
             */
            while (next < valueCount) {
                lastEncoded++;
                writer.add(lastEncoded);
                next++;
            }
            // OrdinalMap has a way of detecting if the delta encoding actually saves space. We skip it for now.
            return ReadFromDisk.provider(decode, writer.readerProvider(), firstNonZeroEncoded);
        }

        @Override
        public void close() throws IOException {
            // NOCOMMIT closing before success should delete file
            IOUtils.close(writer);
        }
    }

    private static class DirectDelegateWriter implements DelegateWriter {
        private final IO io;
        private final IndexOutput out;
        private final DirectWriter writer;
        private final int bitsPerValue;

        DirectDelegateWriter(IO io, long valueCount, int bitsPerValue) throws IOException {
            this.io = io;
            this.bitsPerValue = bitsPerValue;
            out = io.createOutput();
            writer = DirectWriter.getInstance(out, valueCount, bitsPerValue);
        }

        @Override
        public void add(long v) throws IOException {
            writer.add(v);
        }

        @Override
        public DelegateReaderProvider readerProvider() throws IOException {
            writer.finish();
            close();
            String name = out.getName();
            int bitsPerValue = this.bitsPerValue;
            return new DelegateReaderProvider() {
                @Override
                public DelegateReader get() throws IOException {
                    IndexInput in = io.openInput(name);
                    LongValues reader = DirectReader.getInstance(in.randomAccessSlice(0, in.length()), bitsPerValue);
                    return new DelegateReader() {
                        @Override
                        public long get(long index) {
                            return reader.get(index);
                        }

                        @Override
                        public void close() throws IOException {
                            in.close();
                        }
                    };
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.sizeOf(name);
                    size += Integer.BYTES;
                    return RamUsageEstimator.alignObjectSize(size);
                }

                @Override
                public long diskBytesUsed() throws IOException {
                    return io.diskBytesUsed(name);
                }

                @Override
                public void close() throws IOException {
                    io.delete(name);
                }
            };
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class MonotonicDelegateWriter implements DelegateWriter {
        private static final int BLOCK_SHIFT = 16;

        private final IO io;
        private final long valueCount;
        private final ByteBuffersDataOutput meta;
        private final IndexOutput metaOut, dataOut;
        private final DirectMonotonicWriter writer;

        MonotonicDelegateWriter(IO io, long valueCount) throws IOException {
            this.io = io;
            this.valueCount = valueCount;
            meta = new ByteBuffersDataOutput();
            metaOut = new ByteBuffersIndexOutput(meta, "temp", "meta");
            dataOut = io.createOutput();
            writer = DirectMonotonicWriter.getInstance(metaOut, dataOut, valueCount, BLOCK_SHIFT);
        }

        @Override
        public void add(long v) throws IOException {
            writer.add(v);
        }

        @Override
        public DelegateReaderProvider readerProvider() throws IOException {
            writer.finish();
            close();
            DirectMonotonicReader.Meta meta;
            try (IndexInput metaIn = new ByteBuffersIndexInput(new ByteBuffersDataInput(this.meta.toBufferList()), "temp")) {
                meta = DirectMonotonicReader.loadMeta(metaIn, valueCount, BLOCK_SHIFT);
            }
            // OrdinalMap can detect if the delta encoding is larger than just using the direct encoding. We skip that for now.
            String dataName = dataOut.getName();
            return new DelegateReaderProvider() {
                @Override
                public DelegateReader get() throws IOException {
                    IndexInput in = io.openInput(dataName);
                    DirectMonotonicReader reader = DirectMonotonicReader.getInstance(meta, in.randomAccessSlice(0, in.length()));
                    return new DelegateReader() {
                        @Override
                        public long get(long index) {
                            return reader.get(index);
                        }

                        @Override
                        public void close() throws IOException {
                            in.close();
                        }
                    };
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + meta.ramBytesUsed();
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.sizeOf(dataName);
                    return RamUsageEstimator.alignObjectSize(size);
                }

                @Override
                public long diskBytesUsed() throws IOException {
                    return io.diskBytesUsed(dataName);
                }

                @Override
                public void close() throws IOException {
                    io.delete(dataName);
                }
            };
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(metaOut, dataOut);
        }
    }

    /**
     * A sequence that where {@code f(i) == i}.
     */
    private static class Identity implements OrdinalSequence.Reader {
        static final ReaderProvider readerProvider(LongBinaryOperator decode) {
            Identity identity = new Identity(decode);
            return new ReaderProvider() {
                @Override
                public Identity get() throws IOException {
                    return identity;
                }

                @Override
                public long ramBytesUsed() {
                    return RamUsageEstimator.alignObjectSize(
                        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                    );
                }

                @Override
                public long diskBytesUsed() throws IOException {
                    return 0;
                }

                @Override
                public void close() throws IOException {}
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

        @Override
        public void close() throws IOException {}
    }

    private static class ReadFromDisk implements OrdinalSequence.Reader {
        static OrdinalSequence.ReaderProvider provider(
            LongBinaryOperator decode,
            DelegateReaderProvider delegateReaderProvider,
            long firstNonZeroEncoded
        ) throws IOException {
            return new OrdinalSequence.ReaderProvider() {
                @Override
                public ReadFromDisk get() throws IOException {
                    return new ReadFromDisk(decode, delegateReaderProvider.get(), firstNonZeroEncoded);
                }

                @Override
                public long ramBytesUsed() {
                    long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF;
                    size += RamUsageEstimator.NUM_BYTES_OBJECT_REF + delegateReaderProvider.ramBytesUsed();
                    size += Long.BYTES;
                    return RamUsageEstimator.alignObjectSize(size);
                }

                @Override
                public long diskBytesUsed() throws IOException {
                    return delegateReaderProvider.diskBytesUsed();
                }

                @Override
                public void close() throws IOException {
                    delegateReaderProvider.close();
                }
            };
        }

        private final LongBinaryOperator decode;
        private final DelegateReader reader;
        private final long firstNonZeroEncoded;

        ReadFromDisk(LongBinaryOperator decode, DelegateReader reader, long firstNonZeroEncoded) throws IOException {
            this.decode = decode;
            this.reader = reader;
            this.firstNonZeroEncoded = firstNonZeroEncoded;
        }

        @Override
        public long applyAsLong(long index) {
            if (index < firstNonZeroEncoded) {
                return decode.applyAsLong(index, 0);
            }
            return decode.applyAsLong(index, reader.get(index - firstNonZeroEncoded));
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
