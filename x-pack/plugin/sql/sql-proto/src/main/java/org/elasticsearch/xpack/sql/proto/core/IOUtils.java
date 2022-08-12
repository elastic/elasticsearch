/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Trimmed down equivalent of IOUtils in ES core.
 * Provides just the minimal functionality required for the JDBC driver to be independent.
 */
public class IOUtils {
    private IOUtils() {
        // Static utils methods
    }

    public static void close(final Exception ex, final Iterable<? extends Closeable> objects) throws IOException {
        Exception firstException = ex;
        for (final Closeable object : objects) {
            try {
                close(object);
            } catch (final IOException | RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else {
                // since we only assigned an IOException or a RuntimeException to ex above, in this case ex must be a RuntimeException
                throw (RuntimeException) firstException;
            }
        }
    }

    public static void close(final Exception e, final Closeable... objects) throws IOException {
        close(e, Arrays.asList(objects));
    }

    public static void close(@Nullable Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Closes all given {@link Closeable}s, suppressing all thrown exceptions. Some of the {@link Closeable}s may be null, they are ignored.
     *
     * @param objects objects to close
     */
    public static void closeWhileHandlingException(final Closeable... objects) {
        closeWhileHandlingException(Arrays.asList(objects));
    }

    /**
     * Closes all given {@link Closeable}s, suppressing all thrown exceptions.
     *
     * @param objects objects to close
     *
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(final Iterable<? extends Closeable> objects) {
        for (final Closeable object : objects) {
            closeWhileHandlingException(object);
        }
    }

    /**
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(final Closeable closeable) {
        // noinspection EmptyCatchBlock
        try {
            close(closeable);
        } catch (final IOException | RuntimeException e) {}
    }
}
