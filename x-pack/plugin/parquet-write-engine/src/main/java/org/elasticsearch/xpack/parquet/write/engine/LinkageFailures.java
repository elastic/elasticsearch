/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

/**
 * Turns a {@link LinkageError} from the Parquet libraries into an exception.
 *
 * <p>The Parquet writer loads some of its classes late (for example the JTS classes for geospatial statistics), so a class that the node
 * does not provide fails at the first write and not when the plugin loads. The same holds for a class that fails to initialize
 * ({@link ExceptionInInitializerError}) and for a native library that does not load ({@link UnsatisfiedLinkError}). An {@code Error} on a
 * write thread is not handled by the code that completes the listener of a bulk, so the bulk would never get a response. Every place
 * where the engine calls into the Parquet or Iceberg libraries must catch {@code LinkageError} and rethrow the result of
 * {@link #asException}, so that the caller completes with a failure.
 */
final class LinkageFailures {

    private LinkageFailures() {}

    /**
     * @param action what the engine was doing, for example {@code "write a Parquet file"}
     * @param error  the error of the class loader or the linker
     */
    static IllegalStateException asException(String action, LinkageError error) {
        final String detail;
        if (error instanceof NoClassDefFoundError) {
            // The message of this error is the name of the missing class.
            detail = "the class [" + error.getMessage() + "] is not available";
        } else {
            detail = "a library could not be linked or initialized [" + error.getClass().getName() + ": " + error.getMessage() + "]";
        }
        return new IllegalStateException(
            "failed to ["
                + action
                + "] because "
                + detail
                + " in the Parquet write engine; check that the plugins [parquet-write-engine] and [esql-datasource-iceberg] are installed",
            error
        );
    }
}
