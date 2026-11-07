/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Models {@code column LIKE '%google%'}, as ClickBench q21 runs it over {@code URL}: many values laid out back to
 * back in one shared buffer, each offered to {@code contains} (or to the case-insensitive candidate search) with the
 * term {@code "google"}, the way a column reader walks a decompressed chunk.
 *
 * <p>The values are generated the way {@code AnalyticsDocs} in the simdjson benchmarks generates them, from what
 * was measured on the first 100k documents of ClickBench's {@code hits}: {@code URL} and {@code Referer} are
 * mostly non-empty with a mean of about 80-90 bytes and a long exponential tail, and {@code Title} is the long
 * case, a mean of about 226 bytes of Cyrillic words that almost never hold a candidate for an ASCII term.
 *
 * <p>Run it with {@code -p implementation=SCALAR,PANAMA}; on x86 add {@code -jvmArgsAppend -XX:UseAVX=2} (or 3,
 * the default where the CPU has AVX-512) to see the effect of the preferred vector width.
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ContainsUrlBenchmark {

    static {
        BenchmarkLogging.configure();
        VectorizationInfo.printOnce();
    }

    private static final int NUM_VALUES = 8192;
    private static final byte[] TERM = "google".getBytes(StandardCharsets.UTF_8);

    @Param({ "SCALAR", "PANAMA" })
    public VectorImplementation implementation;

    /**
     * Which ClickBench column to model: {@code URL} (99% non-empty, mean 81 bytes), {@code Referer} (96%, mean 89)
     * or {@code Title} (99%, mean 226).
     */
    @Param({ "URL", "Referer", "Title" })
    public String field;

    /** Fraction of values that contain the term. */
    @Param({ "0.02", "0.2" })
    public double matchFraction;

    private ESVectorUtilSupport impl;
    private byte[] blob;
    private int[] offsets;
    private int[] lengths;

    @Setup
    public void setup() {
        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, false).getVectorUtilSupport();
            default -> throw new IllegalArgumentException(implementation.toString());
        };
        int nonEmptyPercent = switch (field) {
            case "URL", "Title" -> 99;
            case "Referer" -> 96;
            default -> throw new IllegalArgumentException(field);
        };
        int meanLength = switch (field) {
            case "URL" -> 81;
            case "Referer" -> 89;
            case "Title" -> 226;
            default -> throw new IllegalArgumentException(field);
        };

        Random random = new Random(42);
        byte[][] values = new byte[NUM_VALUES][];
        int total = 0;
        for (int i = 0; i < NUM_VALUES; i++) {
            if (random.nextInt(100) >= nonEmptyPercent) {
                values[i] = new byte[0];
            } else {
                boolean withTerm = random.nextDouble() < matchFraction;
                values[i] = field.equals("Title") ? title(random, meanLength, withTerm) : url(random, meanLength, withTerm);
            }
            total += values[i].length;
        }
        // Values sit back to back in a single buffer, as they do in a decompressed chunk.
        blob = new byte[total];
        offsets = new int[NUM_VALUES];
        lengths = new int[NUM_VALUES];
        int pos = 0;
        for (int i = 0; i < NUM_VALUES; i++) {
            offsets[i] = pos;
            lengths[i] = values[i].length;
            System.arraycopy(values[i], 0, blob, pos, values[i].length);
            pos += values[i].length;
        }

        // Whatever the implementation, it has to agree with the scalar one.
        ESVectorUtilSupport scalar = ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
        if (contains(scalar) != contains() || mayContainIgnoringAsciiCase(scalar) != containsIgnoringAsciiCase()) {
            throw new AssertionError(implementation + " disagrees with the scalar implementation");
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public int contains() {
        return contains(impl);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_VALUES)
    public int containsIgnoringAsciiCase() {
        return mayContainIgnoringAsciiCase(impl);
    }

    private int contains(ESVectorUtilSupport support) {
        int hits = 0;
        for (int i = 0; i < NUM_VALUES; i++) {
            if (support.contains(blob, offsets[i], lengths[i], TERM, 0, TERM.length)) {
                hits++;
            }
        }
        return hits;
    }

    private int mayContainIgnoringAsciiCase(ESVectorUtilSupport support) {
        int hits = 0;
        for (int i = 0; i < NUM_VALUES; i++) {
            if (support.mayContainIgnoringAsciiCase(blob, offsets[i], lengths[i], TERM, 0, TERM.length)) {
                hits++;
            }
        }
        return hits;
    }

    // ---- test data, after AnalyticsDocs ----

    private static final String ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * A URL the way {@code AnalyticsDocs#url} builds one: scheme, a 6-19 character host with a dot four from the end,
     * then one to four lowercase-alphanumeric path segments, sized from an exponentially distributed target of the
     * given mean. {@code AnalyticsDocs} emits PHP-style {@code \/}; that is one byte once the JSON is decoded, which
     * is what a column stores, so plain {@code /} is used here.
     */
    private static byte[] url(Random random, int meanLength, boolean withTerm) {
        int targetLength = Math.min(20 + (int) (-Math.log(1.0 - random.nextDouble()) * (meanLength - 20)), meanLength * 6);
        StringBuilder sb = new StringBuilder(targetLength + 16);
        sb.append(random.nextInt(100) < 70 ? "http:" : "https:").append("//");
        int hostLength = 6 + random.nextInt(14);
        for (int i = 0; i < hostLength; i++) {
            sb.append(i == hostLength - 4 ? '.' : ALNUM.charAt(random.nextInt(26)));
        }
        int segments = 1 + random.nextInt(4);
        int perSegment = Math.max(3, (targetLength - 12 - hostLength) / segments);
        for (int s = 0; s < segments; s++) {
            sb.append('/');
            int len = Math.max(1, perSegment - perSegment / 2 + random.nextInt(perSegment));
            for (int i = 0; i < len; i++) {
                sb.append(ALNUM.charAt(random.nextInt(ALNUM.length())));
            }
        }
        // The random payload could spell the term by accident; break every occurrence, then add one if wanted.
        int idx;
        while ((idx = sb.indexOf("google")) >= 0) {
            sb.setCharAt(idx, 'x');
        }
        if (withTerm) {
            int at = random.nextInt(sb.length() + 1);
            sb.insert(at, "google");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** A Title the way {@code AnalyticsDocs#title} builds one (without the escape): Cyrillic words separated by spaces. */
    private static byte[] title(Random random, int meanLength, boolean withTerm) {
        int targetBytes = Math.min(20 + (int) (-Math.log(1.0 - random.nextDouble()) * (meanLength - 20)), meanLength * 6);
        StringBuilder sb = new StringBuilder(targetBytes);
        int bytes = 0;
        while (bytes < targetBytes) {
            int letters = 3 + random.nextInt(7);
            for (int i = 0; i < letters; i++) {
                sb.append((char) (0x0430 + random.nextInt(32)));
            }
            bytes += letters * 2;
            if (bytes < targetBytes) {
                sb.append(' ');
                bytes++;
            }
        }
        if (withTerm) {
            // Insert at a word boundary so the UTF-8 stays well-formed.
            int at = sb.indexOf(" ", random.nextInt(sb.length()));
            sb.insert(at < 0 ? sb.length() : at + 1, "google");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
