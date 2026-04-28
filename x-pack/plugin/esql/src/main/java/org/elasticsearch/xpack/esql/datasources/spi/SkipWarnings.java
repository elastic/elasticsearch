/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.logging.HeaderWarning;

/**
 * Collects response {@code Warning} headers for datasource read paths that skip a malformed input
 * and resume processing (non-strict {@link ErrorPolicy}, or similar best-effort fallbacks).
 * <p>
 * The first recorded detail also emits a one-time summary header so clients see a single line
 * describing the overall situation (e.g. "malformed rows were skipped in file X") followed by per-event
 * details. Per-event details are capped at {@link #MAX_ADDED_WARNINGS}; on overflow a single
 * "further warnings suppressed" entry is emitted so clients know more were dropped.
 * <p>
 * Writes go through {@link HeaderWarning#addWarning(String, Object...)} which attaches them to the
 * current thread's response headers; if no thread context is bound (e.g. in unit tests that don't
 * care), the call is a no-op. Instances are stateful and not thread-safe: create one per reader
 * iterator or decoder.
 */
public final class SkipWarnings {

    /** Maximum number of per-event entries recorded; mirrors {@code compute.operator.Warnings}. */
    public static final int MAX_ADDED_WARNINGS = 20;

    private final String summary;
    private int added;
    private boolean summaryEmitted;
    private boolean overflowEmitted;

    public SkipWarnings(String summary) {
        this.summary = summary;
    }

    /**
     * Record a single skip/null-fill event. Emits the summary header on the first call, the detail
     * on this and the next up to {@link #MAX_ADDED_WARNINGS} calls, and a single
     * "further warnings suppressed" header when the cap is exceeded.
     */
    public void add(String detail) {
        if (summaryEmitted == false) {
            HeaderWarning.addWarning(summary);
            summaryEmitted = true;
        }
        if (added < MAX_ADDED_WARNINGS) {
            HeaderWarning.addWarning(detail);
            added++;
        } else if (overflowEmitted == false) {
            HeaderWarning.addWarning("... further warnings suppressed (more than " + MAX_ADDED_WARNINGS + " recorded)");
            overflowEmitted = true;
        }
    }

    /** Number of per-event detail warnings emitted so far (excludes the summary and overflow lines). */
    public int added() {
        return added;
    }
}
