/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.logging.HeaderWarning;

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
 * By default, lines go through {@link HeaderWarning#addWarning(String, Object...)} which attaches
 * them to the current thread's response headers; if no thread context is bound (e.g. in unit tests
 * that don't care), the call is a no-op. Callers that parse on a forked worker thread whose response
 * headers never reach the client (e.g. the parallel-parsing coordinators) instead pass an explicit
 * sink via {@link #of(ErrorPolicy, String, Consumer)} so lines can be routed to a consumer-thread
 * re-emission mechanism; that sink typically applies its own cross-chunk/segment cap on top (see
 * {@code AsyncExternalSourceBuffer#recordReaderWarning}). Because that shared cap is a small, fixed
 * budget split across however many segments/chunks are parsing concurrently, a sink-routed instance
 * caps its own per-event detail count far below {@link #MAX_ADDED_WARNINGS} (see
 * {@link #MAX_ADDED_WARNINGS_PER_SHARED_SINK}) so a single busy segment/chunk cannot exhaust the whole
 * shared budget before any other segment/chunk gets a chance to contribute.
 * Instances are stateful and not thread-safe: create one per reader iterator or decoder.
 * <p>
 * Callers working against an {@link ErrorPolicy} should use {@link #of(ErrorPolicy, String)} to
 * obtain either a live collector or the shared {@link #NOOP} sink, so that call sites never have
 * to null-guard subsequent {@link #add(String)} invocations.
 * <p>
 * This utility lives alongside {@link ErrorPolicy} in the {@code spi} package because datasource
 * plugins may need to emit the same shape of warnings from outside this module; it is a concrete
 * utility rather than an SPI interface.
 */
public class SkipWarnings {

    /** Maximum number of per-event entries recorded; mirrors {@code compute.operator.Warnings}. */
    public static final int MAX_ADDED_WARNINGS = 20;

    /**
     * Per-event detail cap used when this instance's lines are routed to an external sink (see
     * {@link #of(ErrorPolicy, String, Consumer)}), rather than the {@link #MAX_ADDED_WARNINGS} default.
     * That sink is typically shared by many concurrent {@link SkipWarnings} instances — one per
     * parallel-parsing segment or streaming chunk — feeding a single small, central, cross-instance
     * budget (see {@code AsyncExternalSourceBuffer#recordReaderWarning}). Capping each instance's own
     * contribution far below that shared budget means a single segment/chunk with many errors cannot
     * exhaust the whole budget by itself, leaving room for other concurrently-parsed segments/chunks to
     * also be represented.
     */
    public static final int MAX_ADDED_WARNINGS_PER_SHARED_SINK = 4;

    /**
     * Shared sink used when the current {@link ErrorPolicy} never triggers skip/null-fill behavior
     * (e.g. {@link ErrorPolicy#isStrict()}). All {@link #add(String)} calls are silently dropped.
     */
    public static final SkipWarnings NOOP = new SkipWarnings("", HeaderWarning::addWarning, MAX_ADDED_WARNINGS) {
        @Override
        public void add(String detail) {}
    };

    private final String summary;
    private final Consumer<String> sink;
    private final int maxAddedWarnings;
    // Mutable state: not thread-safe, one instance per reader iterator/decoder.
    private int added;
    private boolean summaryEmitted;
    private boolean overflowEmitted;

    public SkipWarnings(String summary) {
        this(summary, HeaderWarning::addWarning, MAX_ADDED_WARNINGS);
    }

    /**
     * @param sink where summary/detail/overflow lines are sent, in place of the default
     *             {@link HeaderWarning#addWarning(String, Object...)}. Used by callers that parse on a
     *             thread whose response headers do not reach the client. Caps this instance's own detail
     *             count at {@link #MAX_ADDED_WARNINGS_PER_SHARED_SINK} rather than
     *             {@link #MAX_ADDED_WARNINGS} — see that constant's javadoc for why.
     */
    public SkipWarnings(String summary, Consumer<String> sink) {
        this(summary, sink, MAX_ADDED_WARNINGS_PER_SHARED_SINK);
    }

    private SkipWarnings(String summary, Consumer<String> sink, int maxAddedWarnings) {
        this.summary = summary;
        this.sink = sink;
        this.maxAddedWarnings = maxAddedWarnings;
    }

    /**
     * Returns {@link #NOOP} for strict policies (which never skip/null-fill and therefore never need
     * to emit a warning), or a fresh live collector seeded with {@code summary} otherwise.
     */
    public static SkipWarnings of(ErrorPolicy policy, String summary) {
        return policy.isStrict() ? NOOP : new SkipWarnings(summary);
    }

    /**
     * As {@link #of(ErrorPolicy, String)}, routing lines through {@code sink} instead of
     * {@link HeaderWarning} directly when the policy is non-strict.
     */
    public static SkipWarnings of(ErrorPolicy policy, String summary, Consumer<String> sink) {
        return policy.isStrict() ? NOOP : new SkipWarnings(summary, sink);
    }

    /**
     * Record a single skip/null-fill event. Emits the summary header on the first call, the detail
     * on this and the next up to this instance's detail cap ({@link #MAX_ADDED_WARNINGS} by default, or
     * {@link #MAX_ADDED_WARNINGS_PER_SHARED_SINK} when constructed with an explicit sink) calls, and a
     * single "further warnings suppressed" header when the cap is exceeded.
     */
    public void add(String detail) {
        if (summaryEmitted == false) {
            // The default sink (HeaderWarning::addWarning) binds via its varargs overload with a zero-length
            // params array, so LoggerMessageFormat#format early-returns and treats the string as plain text;
            // this keeps user data containing '{' or '}' from being reinterpreted as a placeholder pattern.
            sink.accept(summary);
            summaryEmitted = true;
        }
        if (added < maxAddedWarnings) {
            sink.accept(detail);
            added++;
        } else if (overflowEmitted == false) {
            sink.accept("... further warnings suppressed (more than " + maxAddedWarnings + " recorded)");
            overflowEmitted = true;
        }
    }
}
