/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Java FFM bindings for OpenBLAS, exposing single-precision matrix operations
 * ({@code cblas_sgemm}, {@code cblas_sgemv}) built from source as a single
 * runtime-dispatched shared library.
 *
 * <p>The OpenBLAS binary is built with {@code USE_THREAD=0}, so it runs
 * single-threaded. Callers that want parallelism distribute work across multiple
 * calls using ES executors.
 *
 * @see org.elasticsearch.blas.Blas
 */
module org.elasticsearch.blas {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.foreign;
    requires org.elasticsearch.foreign.adapter;

    exports org.elasticsearch.blas to org.elasticsearch.simdvec;

    // `provides org.elasticsearch.foreign.LibraryProvider with ...BlasLibrary$Provider` is
    // injected into module-info.class by the build's augmentForeignModuleInfo task after
    // compilation; do NOT declare it here (the source cannot name the generated class).
}
