/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

/**
 * JDK feature-level constants that do not depend on incubator modules.
 * Separated from {@link PanamaVectorConstants} so that accessing these
 * constants does not trigger class initialization of Panama vector types
 * (which requires the module read for {@code jdk.incubator.vector} to
 * have been established first).
 */
public final class JdkFeatures {

    /**
     * Whether the current JDK supports passing heap-backed MemorySegments to native downcalls.
     * This is true on JDK 22+, where heap segments work with Panama FFM native calls.
     * On JDK 21, heap segments cannot be passed to native function handles.
     */
    public static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    private JdkFeatures() {}
}
