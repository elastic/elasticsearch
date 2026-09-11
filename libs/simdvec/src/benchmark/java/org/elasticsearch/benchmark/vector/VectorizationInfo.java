/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.simdvec.ESVectorizationProvider;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prints which {@link ESVectorizationProvider} the JVM resolved.
 *
 * <p>{@code ESVectorizationProvider.lookup} degrades silently: without a readable
 * {@code jdk.incubator.vector} it returns the scalar default, and without the native library it
 * returns a Panama implementation. Either way a benchmark still produces numbers, just not for the
 * implementation the author meant to measure, and the warnings that explain it go to a logger whose
 * level depends on whatever log4j configuration the benchmark classpath happens to carry. The
 * resolved class name separates the four outcomes unambiguously, on stdout, next to the results.
 *
 * <p>The provider is a JVM-wide singleton, so one line per JMH fork says it for every benchmark in
 * that fork.
 */
public final class VectorizationInfo {

    private static final AtomicBoolean PRINTED = new AtomicBoolean();

    private VectorizationInfo() {}

    /**
     * Prints the resolved provider, at most once per JVM. Safe to call from a static initializer:
     * it forces provider resolution, which every benchmark body would trigger anyway, out of the
     * measured region.
     */
    public static void printOnce() {
        if (PRINTED.compareAndSet(false, true)) {
            print(ESVectorizationProvider.getInstance().getClass().getSimpleName());
        }
    }

    @SuppressForbidden(reason = "names the vectorization implementation that produced the benchmark's numbers")
    private static void print(String provider) {
        System.out.println("[vectorization] provider=" + provider);
    }
}
