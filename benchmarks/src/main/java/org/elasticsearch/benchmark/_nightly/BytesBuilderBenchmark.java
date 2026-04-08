/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.PagedBytes;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Compares the throughput of four byte-buffer builders for writing and reading
 * back integer data:
 * <ul>
 *   <li>{@code paged} – {@link PagedBytesBuilder}, page-cache-backed, circuit-broken</li>
 *   <li>{@code breaking} – {@link BreakingBytesRefBuilder}, single contiguous array, circuit-broken</li>
 *   <li>{@code plain} – Lucene's {@link BytesRefBuilder}, single contiguous array, no breaker</li>
 *   <li>{@code stream} – {@link BytesStreamOutput}, BigArrays-backed stream, no breaker</li>
 * </ul>
 * <p>
 *   The {@code data} parameter controls what is written per invocation, e.g.
 *   {@code 1000_ints} writes 1000 big-endian {@code int} values (4,000 bytes total).
 * </p>
 * <p>
 *   The {@code operation} parameter selects:
 * </p>
 * <ul>
 *   <li>{@code write} – clear the builder then write all data items from scratch</li>
 *   <li>{@code read} – iterate over the pre-written bytes and sum them (no write cost)</li>
 * </ul>
 * <p>
 *   The builders are intentionally not pre-sized per invocation: after the first
 *   iteration each builder has already grown to accommodate the data, so subsequent
 *   {@code write} iterations measure steady-state append throughput without
 *   allocation cost.
 * </p>
 * <p>
 *   The {@link #selfTest()} runs before any benchmark iteration and exercises all
 *   four implementations. This "poisons" operations that are {@code invokevirtual}
 *   but that only use one invocation in each path, forcing the JVM to invoke them
 *   a more prod-like way.
 * </p>
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class BytesBuilderBenchmark {

    private static final VarHandle INT_BIG_ENDIAN = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    static {
        Utils.configureBenchmarkLogging();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod.
            // Confusingly, this makes the test *faster* in some cases and *slower* in others.
            // If you want to know why, ask a wizard. I don't know.
            selfTest();
        }
    }

    @Param({ "paged", "breaking", "plain", "stream" })
    public String impl;

    @Param({ "write", "read" })
    public String operation;

    @Param({ "1000_ints", "4000_ints", "1000_vints", "100_15b_bytes", "100_15b_not_bytes" })
    public String data;

    private long expected;

    private Destination selected;
    private Callable<Long> write;
    private Callable<Long> read;

    @Setup
    public void setup() throws Exception {
        Data data = Data.build(this.data);
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("benchmark");
        int byteSize = (int) data.expectedLength();
        selected = switch (impl) {
            case "paged" -> new Paged(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker, byteSize);
            case "breaking" -> new Breaking(breaker, byteSize);
            case "plain" -> new Plain();
            case "stream" -> new Stream(0);
            default -> throw new IllegalArgumentException("unknown impl: " + impl);
        };
        write = data.writer(selected);
        read = data.reader(selected);
        expected = switch (operation) {
            case "write" -> data.expectedLength();
            case "read" -> data.expectedSum();
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };

        if (operation.equals("read")) {
            // Pre-populate the active builder so that read benchmarks measure only
            // the read path, not write cost.
            write.call();
        }
    }

    @TearDown
    public void teardown() {
        selected.close();
    }

    @Benchmark
    public long run() throws Exception {
        return switch (operation) {
            case "write" -> write.call();
            case "read" -> read.call();
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };
    }

    interface Destination {
        long writeInts(int[] ints) throws IOException;

        long readInts() throws IOException;

        long writeVInts(int[] ints) throws IOException;

        long readVInts() throws IOException;

        long writeBytes(byte[][] chunks) throws IOException;

        long readBytes() throws IOException;

        long writeNotBytes(byte[][] chunks) throws IOException;

        long readNotBytes() throws IOException;

        void close();
    }

    static class Paged implements Destination {

        private final PagedBytesBuilder builder;
        private final PagedBytesCursor scratch = new PagedBytesCursor();

        Paged(PageCacheRecycler recycler, NoopCircuitBreaker breaker, int initialCapacity) {
            builder = new PagedBytesBuilder(recycler, breaker, "benchmark", initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.append(v);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor(scratch);
            long sum = 0;
            while (cursor.remaining() >= Integer.BYTES) {
                sum += cursor.readInt();
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.appendVInt(v);
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor(scratch);
            long sum = 0;
            while (cursor.remaining() > 0) {
                sum += cursor.readVInt();
            }
            return sum;
        }

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.append(chunk, 0, chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor(scratch);
            BytesRef scratch = new BytesRef();
            long sum = 0;
            while (cursor.remaining() > 0) {
                BytesRef ref = cursor.readBytesRef(Math.min(cursor.remaining(), PageCacheRecycler.BYTE_PAGE_SIZE), scratch);
                for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                    sum += ref.bytes[i] & 0xFF;
                }
            }
            return sum;
        }

        @Override
        public long writeNotBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.appendNot(chunk, 0, chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static class Breaking implements Destination {

        private final BreakingBytesRefBuilder builder;

        Breaking(NoopCircuitBreaker breaker, int initialCapacity) {
            builder = new BreakingBytesRefBuilder(breaker, "benchmark", initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + Integer.BYTES);
                INT_BIG_ENDIAN.set(builder.bytes(), builder.length(), v);
                builder.setLength(builder.length() + Integer.BYTES);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            BytesRef ref = builder.bytesRefView();
            long sum = 0;
            for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
                sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + 5);
                builder.setLength(appendVInt(builder.bytes(), builder.length(), v));
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            BytesRef ref = builder.bytesRefView();
            return sumVInts(ref.bytes, ref.offset, ref.offset + ref.length);
        }

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                System.arraycopy(chunk, 0, builder.bytes(), builder.length(), chunk.length);
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            BytesRef ref = builder.bytesRefView();
            long sum = 0;
            for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                sum += ref.bytes[i] & 0xFF;
            }
            return sum;
        }

        @Override
        public long writeNotBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                for (int i = 0; i < chunk.length; i++) {
                    builder.bytes()[builder.length() + i] = (byte) ~chunk[i];
                }
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static class Plain implements Destination {

        private final BytesRefBuilder builder = new BytesRefBuilder();

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + Integer.BYTES);
                INT_BIG_ENDIAN.set(builder.bytes(), builder.length(), v);
                builder.setLength(builder.length() + Integer.BYTES);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            BytesRef ref = builder.get();
            long sum = 0;
            for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
                sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + 5);
                builder.setLength(appendVInt(builder.bytes(), builder.length(), v));
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            BytesRef ref = builder.get();
            return sumVInts(ref.bytes, ref.offset, ref.offset + ref.length);
        }

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                System.arraycopy(chunk, 0, builder.bytes(), builder.length(), chunk.length);
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            BytesRef ref = builder.get();
            long sum = 0;
            for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                sum += ref.bytes[i] & 0xFF;
            }
            return sum;
        }

        @Override
        public long writeNotBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                for (int i = 0; i < chunk.length; i++) {
                    builder.bytes()[builder.length() + i] = (byte) ~chunk[i];
                }
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
        }

        @Override
        public void close() {}
    }

    static class Stream implements Destination {

        private final BytesStreamOutput output;

        Stream(int initialCapacity) {
            output = new BytesStreamOutput(initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) throws IOException {
            output.reset();
            for (int v : ints) {
                output.writeInt(v);
            }
            return output.size();
        }

        /**
         * Note: {@link BytesStreamOutput#bytes()} returns a concrete {@link BytesArray}
         * for small outputs, so {@link BytesReference#streamInput()} and
         * {@link StreamInput#readInt()} here are likely monomorphic — giving this variant an inherent
         * advantage over the others that may not appear in real workloads where multiple implementations
         * are live.
         */
        @Override
        public long readInts() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() >= Integer.BYTES) {
                sum += in.readInt();
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) throws IOException {
            output.reset();
            for (int v : ints) {
                output.writeVInt(v);
            }
            return output.size();
        }

        @Override
        public long readVInts() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() > 0) {
                sum += in.readVInt();
            }
            return sum;
        }

        @Override
        public long writeNotBytes(byte[][] chunks) {
            output.reset();
            for (byte[] chunk : chunks) {
                for (byte b : chunk) {
                    output.writeByte((byte) ~b);
                }
            }
            return output.size();
        }

        @Override
        public long writeBytes(byte[][] chunks) throws IOException {
            output.reset();
            for (byte[] chunk : chunks) {
                output.write(chunk);
            }
            return output.size();
        }

        @Override
        public long readBytes() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() > 0) {
                sum += in.readByte() & 0xFF;
            }
            return sum;
        }

        @Override
        public long readNotBytes() throws IOException {
            return readBytes();
        }

        @Override
        public void close() {}
    }

    void assertResult(String label, long result) {
        if (result != expected) {
            throw new AssertionError("[" + label + "] expected " + expected + " but got " + result);
        }
    }

    private static int vIntSize(int v) {
        int size = 1;
        while ((v & ~0x7F) != 0) {
            size++;
            v >>>= 7;
        }
        return size;
    }

    private static int appendVInt(byte[] bytes, int offset, int v) {
        while ((v & ~0x7F) != 0) {
            bytes[offset++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        bytes[offset++] = (byte) v;
        return offset;
    }

    private static long sumVInts(byte[] bytes, int offset, int end) {
        long sum = 0;
        while (offset < end) {
            byte b = bytes[offset++];
            int v = b & 0x7F;
            if (b < 0) {
                b = bytes[offset++];
                v |= (b & 0x7F) << 7;
                if (b < 0) {
                    b = bytes[offset++];
                    v |= (b & 0x7F) << 14;
                    if (b < 0) {
                        b = bytes[offset++];
                        v |= (b & 0x7F) << 21;
                        if (b < 0) {
                            b = bytes[offset++];
                            v |= (b & 0x0F) << 28;
                        }
                    }
                }
            }
            sum += v;
        }
        return sum;
    }

    static void selfTest() {
        try {
            for (String dataParam : BytesBuilderBenchmark.class.getField("data").getAnnotation(Param.class).value()) {
                for (String implParam : BytesBuilderBenchmark.class.getField("impl").getAnnotation(Param.class).value()) {
                    for (String opParam : BytesBuilderBenchmark.class.getField("operation").getAnnotation(Param.class).value()) {
                        BytesBuilderBenchmark b = new BytesBuilderBenchmark();
                        b.data = dataParam;
                        b.impl = implParam;
                        b.operation = opParam;
                        try {
                            b.setup();
                        } catch (Exception e) {
                            throw new AssertionError("error setting up [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                        }
                        try {
                            b.assertResult(implParam + "/" + opParam + "/" + dataParam, b.run());
                        } catch (IllegalArgumentException e) {
                            // skip invalid (operation, dataType) combos
                        } catch (Exception e) {
                            throw new AssertionError("error running [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                        } finally {
                            b.teardown();
                        }
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    sealed interface Data permits Ints, VInts, Bytes, NotBytes {
        long expectedLength();

        long expectedSum();

        Callable<Long> writer(Destination dest);

        Callable<Long> reader(Destination dest);

        static Data build(String param) {
            // Format: "{count}_{type}" or "{count}x{chunkSize}_{type}"
            int underscore = param.indexOf('_');
            if (underscore < 0) {
                throw new IllegalArgumentException("data param must be '{count}_{type}', got: " + param);
            }
            int count = Integer.parseInt(param.substring(0, underscore));
            String typePart = param.substring(underscore + 1);
            String dataType = parseDataType(typePart);
            Random rng = new Random(42);
            return switch (dataType) {
                case "ints" -> {
                    int[] values = new int[count];
                    for (int i = 0; i < count; i++) {
                        values[i] = rng.nextInt();
                    }
                    yield new Ints(values);
                }
                case "vints" -> {
                    int[] values = new int[count];
                    for (int i = 0; i < count; i++) {
                        // vints are typically small positive values in production (counts, lengths, ordinals)
                        values[i] = rng.nextInt(1 << 14);
                    }
                    yield new VInts(values);
                }
                case "bytes", "not_bytes" -> {
                    String chunkSizeStr = typePart.substring(0, typePart.length() - ("_" + dataType).length());
                    int chunkSize = Math.toIntExact(ByteSizeValue.parseBytesSizeValue(chunkSizeStr, "chunkSize").getBytes());
                    byte[][] chunks = new byte[count][chunkSize];
                    for (byte[] chunk : chunks) {
                        rng.nextBytes(chunk);
                    }
                    yield dataType.equals("bytes") ? new Bytes(chunks) : new NotBytes(chunks);
                }
                default -> throw new IllegalArgumentException("unknown data type: " + typePart);
            };
        }

        private static String parseDataType(String typePart) {
            if (typePart.endsWith("_not_bytes")) {
                return "not_bytes";
            } else if (typePart.endsWith("_bytes")) {
                return "bytes";
            } else {
                return typePart;
            }
        }
    }

    record Ints(int[] values) implements Data {
        @Override
        public long expectedLength() {
            return (long) values.length * Integer.BYTES;
        }

        @Override
        public long expectedSum() {
            long sum = 0;
            for (int v : values) {
                sum += v;
            }
            return sum;
        }

        @Override
        public Callable<Long> writer(Destination dest) {
            return () -> dest.writeInts(values);
        }

        @Override
        public Callable<Long> reader(Destination dest) {
            return dest::readInts;
        }
    }

    record VInts(int[] values) implements Data {
        @Override
        public long expectedLength() {
            long total = 0;
            for (int v : values) {
                total += vIntSize(v);
            }
            return total;
        }

        @Override
        public long expectedSum() {
            long sum = 0;
            for (int v : values) {
                sum += v;
            }
            return sum;
        }

        @Override
        public Callable<Long> writer(Destination dest) {
            return () -> dest.writeVInts(values);
        }

        @Override
        public Callable<Long> reader(Destination dest) {
            return dest::readVInts;
        }
    }

    record Bytes(byte[][] chunks) implements Data {
        @Override
        public long expectedLength() {
            long total = 0;
            for (byte[] chunk : chunks) {
                total += chunk.length;
            }
            return total;
        }

        @Override
        public long expectedSum() {
            long sum = 0;
            for (byte[] chunk : chunks) {
                for (byte b : chunk) {
                    sum += b & 0xFF;
                }
            }
            return sum;
        }

        @Override
        public Callable<Long> writer(Destination dest) {
            return () -> dest.writeBytes(chunks);
        }

        @Override
        public Callable<Long> reader(Destination dest) {
            return dest::readBytes;
        }
    }

    record NotBytes(byte[][] chunks) implements Data {
        @Override
        public long expectedLength() {
            long total = 0;
            for (byte[] chunk : chunks) {
                total += chunk.length;
            }
            return total;
        }

        @Override
        public long expectedSum() {
            long sum = 0;
            for (byte[] chunk : chunks) {
                for (byte b : chunk) {
                    sum += (~b) & 0xFF;
                }
            }
            return sum;
        }

        @Override
        public Callable<Long> writer(Destination dest) {
            return () -> dest.writeNotBytes(chunks);
        }

        @Override
        public Callable<Long> reader(Destination dest) {
            return dest::readNotBytes;
        }
    }
}
