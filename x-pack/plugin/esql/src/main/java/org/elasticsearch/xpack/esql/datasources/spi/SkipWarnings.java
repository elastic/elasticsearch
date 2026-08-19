/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;

import java.util.function.Consumer;

/**
 * Collects response {@code Warning} headers for datasource read paths that skip a malformed input
 * and resume processing (non-strict {@link ErrorPolicy}, or similar best-effort fallbacks).
 * <p>
 * The first recorded detail also emits a one-time summary header so clients see a single line
 * describing the overall situation (e.g. "malformed rows were skipped in file X") followed by per-event
 * details. Per-event details are capped at {@link #MAX_ADDED_WARNINGS}; on overflow a single
 * "further warnings suppressed" entry is emitted so clients know more were dropped.
 * <p>
 * By default, writes go through {@link HeaderWarning#addWarning(String, Object...)} which attaches
 * them to the current thread's response headers; if no thread context is bound (e.g. in unit tests
 * that don't care), the call is a no-op. This is only correct when {@link #add(String)} is called on
 * a thread whose {@code ThreadContext} response headers actually feed the client response (e.g. the
 * originating request thread). Readers whose decode loop can run on a different thread (e.g. a
 * background reader thread wrapped by {@code AsyncExternalSourceOperatorFactory}) must instead supply
 * a {@code sink} — typically {@code AsyncExternalSourceBuffer::recordInformationalWarning} — via
 * {@link #SkipWarnings(String, Consumer)} / {@link #of(ErrorPolicy, String, Consumer)} so the message
 * is relayed and re-emitted on the correct thread instead of being silently dropped. Instances are
 * stateful and not thread-safe: create one per reader iterator or decoder.
 * <p>
 * Callers working against an {@link ErrorPolicy} should use {@link #of(ErrorPolicy, String)} (or the
 * sink-aware {@link #of(ErrorPolicy, String, Consumer)}) to obtain either a live collector or the
 * shared {@link #NOOP} sink, so that call sites never have to null-guard subsequent {@link #add(String)}
 * invocations.
 * <p>
 * This utility lives alongside {@link ErrorPolicy} in the {@code spi} package because datasource
 * plugins may need to emit the same shape of warnings from outside this module; it is a concrete
 * utility rather than an SPI interface.
 */
public class SkipWarnings {

    /** Maximum number of per-event entries recorded; mirrors {@code compute.operator.Warnings}. */
    public static final int MAX_ADDED_WARNINGS = 20;

    /**
     * The single "further warnings suppressed" line emitted once per collector when the per-event
     * cap is exceeded. Exposed as the one source of truth for the overflow text so a central budget
     * that caps the same channel (see {@code InformationalWarningBudget}) emits a byte-identical
     * marker: identical text dedups against a per-collector overflow line by value, and clients that
     * match on "further warnings suppressed" keep working regardless of which layer capped.
     */
    public static String overflowMessage() {
        return OVERFLOW_MESSAGE;
    }

    private static final String OVERFLOW_MESSAGE = "... further warnings suppressed (more than " + MAX_ADDED_WARNINGS + " recorded)";

    /**
     * Shared sink used when the current {@link ErrorPolicy} never triggers skip/null-fill behavior
     * (e.g. {@link ErrorPolicy#isStrict()}). All {@link #add(String)} calls are silently dropped.
     */
    public static final SkipWarnings NOOP = new SkipWarnings("") {
        @Override
        public void add(String detail) {}
    };

    private final String summary;
    /**
     * Where emitted messages go. {@code null} (the default) preserves the original direct-to-
     * {@link HeaderWarning} behavior, which is only safe on a thread whose response headers are
     * actually collected into the client response.
     */
    @Nullable
    private final Consumer<String> sink;
    // Mutable state: not thread-safe, one instance per reader iterator/decoder.
    private int added;
    private boolean summaryEmitted;
    private boolean overflowEmitted;

    public SkipWarnings(String summary) {
        this(summary, null);
    }

    /**
     * @param sink when non-{@code null}, every emitted message is handed to this consumer instead of
     *             {@link HeaderWarning#addWarning(String, Object...)}. Use this on any code path whose
     *             {@link #add(String)} calls may run off the request/driver thread.
     */
    public SkipWarnings(String summary, @Nullable Consumer<String> sink) {
        this.summary = summary;
        this.sink = sink;
    }

    /**
     * Returns {@link #NOOP} for strict policies (which never skip/null-fill and therefore never need
     * to emit a warning), or a fresh live collector seeded with {@code summary} otherwise.
     */
    public static SkipWarnings of(ErrorPolicy policy, String summary) {
        return of(policy, summary, null);
    }

    /**
     * Like {@link #of(ErrorPolicy, String)}, but routes emitted messages through {@code sink} instead
     * of directly through {@link HeaderWarning}. See {@link #SkipWarnings(String, Consumer)}.
     */
    public static SkipWarnings of(ErrorPolicy policy, String summary, @Nullable Consumer<String> sink) {
        return policy.isStrict() ? NOOP : new SkipWarnings(summary, sink);
    }

    /**
     * Record a single skip/null-fill event. Emits the summary header on the first call, the detail
     * on this and the next up to {@link #MAX_ADDED_WARNINGS} calls, and a single
     * "further warnings suppressed" header when the cap is exceeded.
     */
    public void add(String detail) {
        if (summaryEmitted == false) {
            // Use the no-varargs overload so HeaderWarning treats both summary and detail as plain
            // strings (LoggerMessageFormat#format early-returns when argArray is empty); this keeps
            // user data containing '{' or '}' from being reinterpreted as a placeholder pattern.
            emit(summary);
            summaryEmitted = true;
        }
        if (added < MAX_ADDED_WARNINGS) {
            emit(detail);
            added++;
        } else if (overflowEmitted == false) {
            emit(overflowMessage());
            overflowEmitted = true;
        }
    }

    private void emit(String message) {
        if (sink != null) {
            sink.accept(message);
        } else {
            HeaderWarning.addWarning(message);
        }
    }
}
