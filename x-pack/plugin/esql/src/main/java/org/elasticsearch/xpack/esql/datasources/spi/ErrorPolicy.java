/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Locale;

/**
 * Configures how format readers handle malformed or unparseable rows.
 *
 * <h2>Error modes</h2>
 * <table>
 *   <caption>Error mode comparison across engines</caption>
 *   <tr><th>ES/ESQL</th><th>Spark</th><th>DuckDB</th><th>ClickHouse</th><th>Behaviour</th></tr>
 *   <tr><td>{@link Mode#FAIL_FAST FAIL_FAST}</td><td>FAILFAST</td><td>(default)</td>
 *       <td>errors_num=0</td><td>Abort on first error</td></tr>
 *   <tr><td>{@link Mode#SKIP_ROW SKIP_ROW}</td><td>DROPMALFORMED</td><td>ignore_errors</td>
 *       <td>errors_num&gt;0</td><td>Drop the entire bad row</td></tr>
 *   <tr><td>{@link Mode#NULL_FIELD NULL_FIELD}</td><td>PERMISSIVE</td><td>—</td>
 *       <td>—</td><td>Null-fill unparseable fields, keep the row</td></tr>
 * </table>
 *
 * <h2>Error budget</h2>
 * {@code maxErrors} and {@code maxErrorRatio} only apply to {@link Mode#SKIP_ROW} and
 * {@link Mode#NULL_FIELD}. They are ignored in {@link Mode#FAIL_FAST} (which always aborts
 * on the first error).  When both are set, whichever limit is hit first triggers failure.
 * <table>
 *   <caption>Error budget comparison across engines</caption>
 *   <tr><th>ES/ESQL</th><th>DuckDB</th><th>ClickHouse</th></tr>
 *   <tr><td>{@code max_errors}</td><td>rejects_limit</td>
 *       <td>input_format_allow_errors_num</td></tr>
 *   <tr><td>{@code max_error_ratio}</td><td>—</td>
 *       <td>input_format_allow_errors_ratio</td></tr>
 * </table>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 *   FROM s3://bucket/data.csv WITH {"max_errors": 100}
 *   FROM s3://bucket/data.csv WITH {"error_mode": "skip_row", "max_error_ratio": 0.1}
 *   FROM s3://bucket/data.csv WITH {"error_mode": "null_field"}
 * }</pre>
 *
 * @param mode           how to handle rows with parse errors
 * @param maxErrors      maximum number of errors to tolerate before failing
 *                       (only meaningful for {@code SKIP_ROW} and {@code NULL_FIELD})
 * @param maxErrorRatio  maximum fraction of rows (0.0–1.0) that may be errors before
 *                       failing; 0.0 disables ratio-based tolerance
 *                       (only meaningful for {@code SKIP_ROW} and {@code NULL_FIELD})
 * @param logErrors      whether to log each skipped/null-filled row at WARN level
 */
public record ErrorPolicy(Mode mode, long maxErrors, double maxErrorRatio, boolean logErrors) {

    /**
     * Determines what happens to a row (or field) that fails to parse.
     */
    public enum Mode {
        /** Abort immediately — equivalent to Spark {@code FAILFAST}. */
        FAIL_FAST,
        /** Drop the entire row — equivalent to Spark {@code DROPMALFORMED}, DuckDB {@code ignore_errors}. */
        SKIP_ROW,
        /** Null-fill unparseable fields, keep the row — equivalent to Spark {@code PERMISSIVE}. */
        NULL_FIELD;

        /**
         * Case-insensitive parse; accepts underscore form ({@code skip_row}).
         * Returns {@code null} for null/empty input.
         */
        public static Mode parse(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            String normalized = value.toUpperCase(Locale.ROOT).replace(" ", "_");
            return Mode.valueOf(normalized);
        }
    }

    /** Fail immediately on any malformed row. */
    public static final ErrorPolicy STRICT = new ErrorPolicy(Mode.FAIL_FAST, 0, 0.0, false);

    /** Skip all malformed rows without limit, logging each one. */
    public static final ErrorPolicy LENIENT = new ErrorPolicy(Mode.SKIP_ROW, Long.MAX_VALUE, 1.0, true);

    public ErrorPolicy {
        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null");
        }
        if (maxErrors < 0) {
            throw new IllegalArgumentException("maxErrors must be non-negative, got: " + maxErrors);
        }
        if (maxErrorRatio < 0.0 || maxErrorRatio > 1.0) {
            throw new IllegalArgumentException("maxErrorRatio must be between 0.0 and 1.0, got: " + maxErrorRatio);
        }
    }

    /**
     * Convenience constructor for {@link Mode#SKIP_ROW} without ratio-based tolerance.
     */
    public ErrorPolicy(long maxErrors, boolean logErrors) {
        this(Mode.SKIP_ROW, maxErrors, 0.0, logErrors);
    }

    /**
     * Convenience constructor for {@link Mode#SKIP_ROW} with ratio-based tolerance.
     */
    public ErrorPolicy(long maxErrors, double maxErrorRatio, boolean logErrors) {
        this(Mode.SKIP_ROW, maxErrors, maxErrorRatio, logErrors);
    }

    public boolean isStrict() {
        return mode == Mode.FAIL_FAST;
    }

    public boolean isPermissive() {
        return mode == Mode.NULL_FIELD;
    }

    /**
     * Checks whether the error budget has been exceeded given the current counts.
     * Always returns {@code false} for {@link Mode#FAIL_FAST} (which never reaches
     * this check — it throws immediately in the caller).
     *
     * @param errorsSoFar  total errors encountered so far
     * @param rowsSoFar    total rows processed so far (including errors)
     * @return true if the budget is exceeded and processing should stop
     */
    public boolean isBudgetExceeded(long errorsSoFar, long rowsSoFar) {
        if (errorsSoFar > maxErrors) {
            return true;
        }
        if (maxErrorRatio > 0.0 && rowsSoFar > 0) {
            return (double) errorsSoFar > maxErrorRatio * rowsSoFar;
        }
        return false;
    }
}
