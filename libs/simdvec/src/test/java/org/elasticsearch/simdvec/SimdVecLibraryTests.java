/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;

public abstract class SimdVecLibraryTests extends ESTestCase {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    public static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;
    public static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    protected static SimdVecLibrary vectorSimilarityFunctions;
    protected static Arena arena;

    protected final SimdVecLibrary.SimilarityFunction function;
    protected final int size;

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        var dims1 = Arrays.stream(new int[] { 1, 2, 4, 6, 8, 12, 13, 16, 25, 31, 32, 33, 64, 100, 128, 207, 256, 300, 512, 702, 768 });
        var dims2 = Arrays.stream(new int[] { 1000, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097 });
        return () -> IntStream.concat(dims1, dims2)
            .boxed()
            .flatMap(i -> Stream.of(SimdVecLibrary.SimilarityFunction.values()).map(f -> new Object[] { f, i }))
            .iterator();
    }

    protected SimdVecLibraryTests(SimdVecLibrary.SimilarityFunction function, int size) {
        this.function = function;
        this.size = size;

        logger.info(platformMsg());
    }

    public static void setup() {
        var simdVecSupported = supported();
        if (simdVecSupported) {
            vectorSimilarityFunctions = SimdVecLibrary.instance().orElse(null);
            assertNotNull("native vector library must be available on [" + platformMsg() + "]", vectorSimilarityFunctions);
        }
        assumeTrue(notSupportedMsg(), simdVecSupported);

        // Occasionally back every segment of this suite with a guard page, so that a native over-read faults
        // instead of silently returning a wrong score.
        var useGuardPageAllocator = randomBoolean();
        arena = GuardPageAllocator.isSupported() && useGuardPageAllocator ? GuardPageAllocator.ofConfined() : Arena.ofConfined();
    }

    public static void cleanup() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    protected SimdVecLibrary getVectorDistance() {
        return vectorSimilarityFunctions;
    }

    public static boolean supported() {
        return SimdVecLibrary.isNativeVectorLibSupported() && VecCaps.caps() > 0;
    }

    public static String notSupportedMsg() {
        return "Not supported on [" + platformMsg() + "]";
    }

    public static String platformMsg() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        return "JDK=" + jdkVersion + ", os=" + osName + ", arch=" + arch;
    }

    protected static RuntimeException rethrow(Throwable t) {
        if (t instanceof Error err) {
            throw err;
        }
        return t instanceof RuntimeException re ? re : new RuntimeException(t);
    }

    public static float[] randomFloatArray(int length) {
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    }

    protected static void assertScoresEquals(float[] expectedScores, MemorySegment expectedScoresSeg) {
        assertScoresEquals(expectedScores, expectedScoresSeg, 0f);
    }

    protected static void assertScoresEquals(float[] expectedScores, MemorySegment expectedScoresSeg, float delta) {
        assert expectedScores.length == (expectedScoresSeg.byteSize() / Float.BYTES);
        for (int i = 0; i < expectedScores.length; i++) {
            assertEquals(
                "Difference at offset " + i,
                expectedScores[i],
                expectedScoresSeg.get(JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES),
                delta
            );
        }
    }
}
