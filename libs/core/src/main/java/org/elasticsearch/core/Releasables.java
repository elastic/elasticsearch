/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/** Utility methods to work with {@link Releasable}s. */
public enum Releasables {
    ;

    /** Release the provided {@link Releasable}s. */
    public static void close(Iterable<? extends Releasable> releasables) {
        try {
            // this does the right thing with respect to add suppressed and not wrapping errors etc.
            IOUtils.close(releasables);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        close(true, releasables);
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
        close(false, releasables);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is {@code false}. */
    private static void close(boolean success, Releasable... releasables) {
        try {
            // this does the right thing with respect to add suppressed and not wrapping errors etc.
            IOUtils.close(releasables);
        } catch (IOException e) {
            if (success) {
                throw new UncheckedIOException(e);
            }
        }
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
        return new Releasable() {
            @Override
            public void close() {
                Releasables.close(releasables);
            }

            @Override
            public String toString() {
                return "wrapped[" + releasables + "]";
            }
        };
    }

    /** @see #wrap(Iterable) */
    public static Releasable wrap(final Releasable... releasables) {
        return new Releasable() {
            @Override
            public void close() {
                Releasables.close(releasables);
            }

            @Override
            public String toString() {
                return "wrapped" + Arrays.toString(releasables);
            }
        };
    }

    /**
     * Wraps a {@link Releasable} such that its {@link Releasable#close()} method can be called multiple times without double-releasing.
     */
    public static Releasable releaseOnce(final Releasable releasable) {
        return new ReleaseOnce(releasable);
    }

    public static Releasable assertOnce(final Releasable delegate) {
        if (Assertions.ENABLED) {
            return new Releasable() {
                // if complete, records the stack trace which first completed it
                private final AtomicReference<Exception> firstCompletion = new AtomicReference<>();

                private void assertFirstRun() {
                    var previousRun = firstCompletion.compareAndExchange(null, new Exception(delegate.toString()));
                    assert previousRun == null : previousRun; // reports the stack traces of both completions
                }

                @Override
                public void close() {
                    assertFirstRun();
                    delegate.close();
                }

                @Override
                public String toString() {
                    return delegate.toString();
                }
            };
        } else {
            return delegate;
        }
    }

    private static class ReleaseOnce extends AtomicReference<Releasable> implements Releasable {
        ReleaseOnce(Releasable releasable) {
            super(releasable);
        }

        @Override
        public void close() {
            final var acquired = getAndSet(null);
            if (acquired != null) {
                acquired.close();
            }
        }

        @Override
        public String toString() {
            return "releaseOnce[" + get() + "]";
        }
    }
}
