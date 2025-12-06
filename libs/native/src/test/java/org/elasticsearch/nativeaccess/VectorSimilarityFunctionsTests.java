/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.not;

public abstract class VectorSimilarityFunctionsTests extends ESTestCase {

    static {
        NodeNamePatternConverter.setGlobalNodeName("foo");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    public static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;
    public static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    protected static Arena arena;

    protected final int size;
    protected final Optional<VectorSimilarityFunctions> vectorSimilarityFunctions;

    protected static Iterable<Object[]> parametersFactory() {
        var dims1 = Arrays.stream(new int[] { 1, 2, 4, 6, 8, 12, 13, 16, 25, 31, 32, 33, 64, 100, 128, 207, 256, 300, 512, 702, 768 });
        var dims2 = Arrays.stream(new int[] { 1000, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097 });
        return () -> IntStream.concat(dims1, dims2).boxed().map(i -> new Object[] { i }).iterator();
    }

    protected VectorSimilarityFunctionsTests(int size) {
        logger.info(platformMsg());
        this.size = size;
        vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions();
    }

    public static void setup() {
        arena = Arena.ofConfined();
    }

    public static void cleanup() {
        arena.close();
    }

    public void testSupported() {
        supported();
    }

    protected VectorSimilarityFunctions getVectorDistance() {
        return vectorSimilarityFunctions.get();
    }

    public boolean supported() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if (jdkVersion >= 21
            && ((arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux")))
                || (arch.equals("amd64") && osName.equals("Linux")))) {
            assertThat(vectorSimilarityFunctions, isPresent());
            return true;
        } else {
            assertThat(vectorSimilarityFunctions, not(isPresent()));
            return false;
        }
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

    // Support for passing on-heap arrays/segments to native
    protected static boolean supportsHeapSegments() {
        return Runtime.version().feature() >= 22;
    }
}
