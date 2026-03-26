/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Contains Panama and native SIMD implementations of various vector operations.
 * <p>
 * The native code is contained in C++ files, with implementations for ARM and two generations of x86,
 * using significant amounts of CPU intrinsics to utilise specific SIMD operations.
 * The method handles are loaded using FFI, and made available through a series
 * of wrapper classes to be called from Elasticsearch-defined vector formats.
 * <p>
 * A small number of JDK 22-specific implementations remain in the {@code main22} source set,
 * packaged as a multi-release JAR. On JDK 22+, the multi-release classloader selects
 * those implementations over the {@code main} versions.
 */
module org.elasticsearch.simdvec {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.nativeaccess;
    requires org.apache.lucene.core;

    exports org.elasticsearch.simdvec to org.elasticsearch.server;
}
