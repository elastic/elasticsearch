/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/** Utility methods to work with {@link Releasable}s. */
public enum Releasables {
    ;

    private static void close(Iterable<? extends Releasable> releasables, boolean ignoreException) {
        try {
            // this does the right thing with respect to add suppressed and not wrapping errors etc.
            IOUtils.close(releasables);
        } catch (IOException e) {
            if (ignoreException == false) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Iterable<? extends Releasable> releasables) {
        close(releasables, false);
    }

    /** Release the provided {@link Releasable}. */
    public static void close(@Nullable Releasable releasable) {
        try {
            IOUtils.close(releasable);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Releasable... releasables) {
        close(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s expecting no exception to by thrown by any of them. */
    public static void closeExpectNoException(Releasable... releasables) {
        try {
            close(releasables);
        } catch (RuntimeException e) {
            assert false : e;
            throw e;
        }
    }

    /** Release the provided {@link Releasable} expecting no exception to by thrown. */
    public static void closeExpectNoException(Releasable releasable) {
        try {
            close(releasable);
        } catch (RuntimeException e) {
            assert false : e;
            throw e;
        }
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Releasable... releasables) {
        close(Arrays.asList(releasables), true);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is {@code false}. */
    public static void close(boolean success, Iterable<Releasable> releasables) {
        close(releasables, success == false);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is {@code false}. */
    public static void close(boolean success, Releasable... releasables) {
        close(success, Arrays.asList(releasables));
    }

    /** Wrap several releasables into a single one. This is typically useful for use with try-with-resources: for example let's assume
     *  that you store in a list several resources that you would like to see released after execution of the try block:
     *
     *  <pre>
     *  List&lt;Releasable&gt; resources = ...;
     *  try (Releasable releasable = Releasables.wrap(resources)) {
     *      // do something
     *  }
     *  // the resources will be released when reaching here
     *  </pre>
     */
    public static Releasable wrap(final Iterable<Releasable> releasables) {
        return () -> close(releasables);
    }

    /** @see #wrap(Iterable) */
    public static Releasable wrap(final Releasable... releasables) {
        return () -> close(releasables);
    }

    /**
     * Wraps a {@link Releasable} such that its {@link Releasable#close()} method can be called multiple times without double releasing.
     */
    public static Releasable releaseOnce(final Releasable releasable) {
        final AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                releasable.close();
            }
        };
    }
}
