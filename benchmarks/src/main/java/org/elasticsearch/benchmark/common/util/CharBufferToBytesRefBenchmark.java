/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.lucene.BytesRefs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the ways to turn the {@link CharBuffer} that a JSON parser hands out for a string value into a {@link BytesRef}, as a parsed
 * {@code terms} query does once per term:
 * <ul>
 * <li>{@code newBytesRef}: {@code new BytesRef(CharSequence)}, one pass, but the array is sized for 3 bytes per char.</li>
 * <li>{@code exactTwoPass}: {@link BytesRefs#toExactSizedBytesRef}, which measures the UTF-8 length and then encodes.</li>
 * <li>{@code allocThenTrim}: as {@code newBytesRef}, then copy out exactly the bytes used, so a 3x array is allocated per value.</li>
 * <li>{@code asciiFastThenScratch}: a vectorizable ASCII check, with a narrowing copy if it passes, else as {@code exactScratchCopy}.</li>
 * <li>{@code exactScratchCopy}: encode once into a reused scratch array, then copy out exactly the bytes used.</li>
 * </ul>
 * The {@code charset} parameter picks how many UTF-8 bytes each char takes, from 1 (ASCII) to 4 per surrogate pair, so that the cost is
 * not measured on 1-byte data only. Run with {@code -prof gc} to see the allocation difference.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class CharBufferToBytesRefBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /** Conversions per invocation. */
    private static final int NUM_VALUES = 1024;
    /** Chars of distinct input to cycle through, so long terms do not make the working set exceed the cache. */
    private static final int POOL_CHARS = 1 << 20;

    /** Length in chars, so a surrogate pair counts as 2. The last is {@code IndexWriter.MAX_TERM_LENGTH}, the longest the encoder takes. */
    @Param({ "20", "32", "64", "512", "1024", "16384", "32766" })
    public int length;

    /**
     * ascii: 1 byte per char. latin: 2 bytes per char. cjk: 3 bytes per char. emoji: surrogate pairs, 4 bytes per 2 chars. mixed: mostly
     * ASCII with an occasional char from each of the other classes.
     */
    @Param({ "ascii", "latin", "cjk", "emoji", "mixed" })
    public String charset;

    private CharBuffer[] values;
    private int mask;
    private byte[] scratch;

    @Setup
    public void setup() {
        Random random = new Random(1);
        // The parser wraps a slice of its own char[], so the buffers do not start at position 0 of an array of their own.
        // a power of two, so that the benchmarks can wrap around with a mask
        int count = Integer.highestOneBit(Math.max(1, Math.min(NUM_VALUES, POOL_CHARS / length)));
        mask = count - 1;
        char[] backing = new char[count * (length + 1)];
        values = new CharBuffer[count];
        int offset = 0;
        for (int i = 0; i < count; i++) {
            fill(backing, offset, length, random);
            values[i] = CharBuffer.wrap(backing, offset, length);
            offset += length + 1;
        }
        scratch = new byte[UnicodeUtil.maxUTF8Length(length)];
    }

    private void fill(char[] out, int offset, int length, Random random) {
        int i = 0;
        while (i < length) {
            String kind = charset.equals("mixed") ? mixedKind(random) : charset;
            switch (kind) {
                case "ascii" -> out[offset + i++] = (char) ('a' + random.nextInt(26));
                case "latin" -> out[offset + i++] = (char) (0xE0 + random.nextInt(0x1F));
                case "cjk" -> out[offset + i++] = (char) (0x4E00 + random.nextInt(0x1000));
                case "emoji" -> {
                    if (i + 1 < length) {
                        int codePoint = 0x1F600 + random.nextInt(0x40);
                        i += Character.toChars(codePoint, out, offset + i);
                    } else {
                        // a pair does not fit in the last char
                        out[offset + i++] = 'a';
                    }
                }
                default -> throw new IllegalArgumentException("unknown charset [" + kind + "]");
            }
        }
    }

    private static String mixedKind(Random random) {
        int roll = random.nextInt(100);
        if (roll < 85) {
            return "ascii";
        } else if (roll < 92) {
            return "latin";
        } else if (roll < 97) {
            return "cjk";
        }
        return "emoji";
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void newBytesRef(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            bh.consume(new BytesRef(values[i & mask]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void exactTwoPass(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            bh.consume(BytesRefs.toExactSizedBytesRef(values[i & mask]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void allocThenTrim(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            CharBuffer value = values[i & mask];
            byte[] bytes = new byte[UnicodeUtil.maxUTF8Length(value.length())];
            int used = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), bytes);
            bh.consume(new BytesRef(Arrays.copyOf(bytes, used)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void asciiFastThenScratch(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            bh.consume(asciiFastThenScratch(values[i & mask], scratch));
        }
    }

    /**
     * Checks whether the chars are all ASCII with a branch-free OR over the backing array, which the JIT can vectorize. If they are, the
     * exact size is the char count and the bytes are a narrowing copy. Otherwise, or if the buffer has no accessible array, it encodes
     * into {@code scratch} and copies out what was used.
     */
    static BytesRef asciiFastThenScratch(CharBuffer value, byte[] scratch) {
        int length = value.length();
        if (value.hasArray()) {
            char[] chars = value.array();
            int from = value.arrayOffset() + value.position();
            int bits = 0;
            for (int i = from; i < from + length; i++) {
                bits |= chars[i];
            }
            if ((bits & 0xFF80) == 0) {
                byte[] bytes = new byte[length];
                for (int i = 0; i < length; i++) {
                    bytes[i] = (byte) chars[from + i];
                }
                return new BytesRef(bytes);
            }
        }
        int used = UnicodeUtil.UTF16toUTF8(value, 0, length, scratch);
        return new BytesRef(Arrays.copyOf(scratch, used));
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void fusedAsciiCopy(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            bh.consume(fusedAsciiCopy(values[i & mask], scratch));
        }
    }

    /**
     * {@code TermEncoder.encode2}: narrows the chars into a byte array while OR-ing them, in one pass, and keeps it if all were ASCII.
     * Otherwise the array is wasted and it encodes into {@code scratch}.
     */
    static BytesRef fusedAsciiCopy(CharBuffer value, byte[] scratch) {
        int length = value.length();
        if (value.hasArray() == false) {
            int used = UnicodeUtil.UTF16toUTF8(value, 0, length, scratch);
            return new BytesRef(Arrays.copyOf(scratch, used));
        }
        char[] chars = value.array();
        int from = value.arrayOffset() + value.position();
        byte[] bytes = new byte[length];
        int bits = 0;
        for (int i = 0; i < length; i++) {
            char c = chars[from + i];
            bits |= c;
            bytes[i] = (byte) c;
        }
        if (bits < 0x80) {
            return new BytesRef(bytes);
        }
        int used = UnicodeUtil.UTF16toUTF8(chars, from, length, scratch);
        return new BytesRef(Arrays.copyOf(scratch, used));
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public void exactScratchCopy(Blackhole bh) {
        for (int i = 0; i < NUM_VALUES; i++) {
            CharBuffer value = values[i & mask];
            int bytes = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), scratch);
            bh.consume(new BytesRef(Arrays.copyOf(scratch, bytes)));
        }
    }
}
