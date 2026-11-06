/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Binds a {@link Scope} to the current thread and gives a codec the recorder of the bound scope. A frontend reaches a
 * codec through Lucene calls such as {@code LeafReader#getBinaryDocValues}, which carry no context, so the scope is
 * held in a thread-local. A frontend binds a scope around the code that opens readers. A codec calls {@link #current}
 * once for each reader it opens.
 */
public final class Scopes {

    private static final ThreadLocal<Scope> BOUND = new ThreadLocal<>();

    private Scopes() {}

    /**
     * Makes {@code scope} the bound scope of this thread until the returned binding is closed. Bindings nest: closing a
     * binding restores the scope bound before it, so a reader opened in an inner scope does not report to an outer
     * scope. Close a binding on the thread that bound it, innermost first.
     */
    public static Binding bind(Scope scope) {
        final Scope previous = BOUND.get();
        BOUND.set(scope);
        return new Binding(scope, previous);
    }

    /**
     * The recorder for a reader that the doc values format {@code codec} opens now on this thread, for {@code field} of
     * segment {@code segment}. Returns null when no scope is bound; the reader is then not profiled. The recorder
     * belongs to the execution of the bound scope, so only objects whose work belongs to that execution may keep it; the
     * module documentation states the rule.
     *
     * @param facts what the codec knows about the stored column; see {@link Scope#metrics}
     */
    @Nullable
    public static MetricRecorder current(String codec, String segment, String field, Map<String, Boolean> facts) {
        final Scope scope = BOUND.get();
        return scope == null ? null : scope.metrics(codec, segment, field, facts);
    }

    /** A scope whose readers all report to {@code metrics}. */
    static Scope eager(MetricRecorder metrics) {
        return (codec, segment, field, facts) -> metrics;
    }

    /** Whether a scope is bound on this thread. A binding that stays open after its work charges unrelated readers to it. */
    static boolean isBound() {
        return BOUND.get() != null;
    }

    /** A scope bound on a thread. Closing it restores the scope bound before it. A second close does nothing. */
    public static final class Binding implements AutoCloseable {
        private final Scope bound;
        @Nullable
        private final Scope previous;
        private final Thread owner = Thread.currentThread();
        private boolean closed;

        private Binding(Scope bound, @Nullable Scope previous) {
            this.bound = bound;
            this.previous = previous;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            assert Thread.currentThread() == owner : "a binding is closed on the thread that bound it";
            assert BOUND.get() == bound : "bindings are closed innermost first";
            closed = true;
            if (previous == null) {
                BOUND.remove();
            } else {
                BOUND.set(previous);
            }
        }
    }
}
