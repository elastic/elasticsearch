/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Locale;
import java.util.Map;

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
 * {@snippet lang="esql" :
 *   FROM s3://bucket/data.csv WITH {"max_errors": 100}
 *   FROM s3://bucket/data.csv WITH {"error_mode": "skip_row", "max_error_ratio": 0.1}
 *   FROM s3://bucket/data.csv WITH {"error_mode": "null_field"}
 * }
 *
 * <h2>Client-visible warnings</h2>
 * Whenever the non-strict modes ({@link Mode#SKIP_ROW} and {@link Mode#NULL_FIELD}) cause a row to be
 * dropped or a field to be null-filled, format readers emit response {@code Warning} headers via
 * {@link SkipWarnings}: a one-time summary identifying the file, plus per-event details capped at
 * {@link SkipWarnings#MAX_ADDED_WARNINGS} entries (further events collapse into a single
 * "further warnings suppressed" line). This is independent of {@code logErrors}, which still only
 * controls server-side WARN logging.
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
     * Lowercase, locale-insensitive name of {@link #mode()}, suitable for user-facing messages
     * (e.g. {@code "skip_row"}, {@code "null_field"}). Matches the parser input accepted by
     * {@link Mode#parse(String)}.
     */
    public String modeName() {
        return mode.name().toLowerCase(Locale.ROOT);
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

    /** Config keys recognised by {@link #fromConfig}. Mirrored as constants so format
     *  plugins do not have to hard-code the strings. */
    public static final String CONFIG_MAX_ERRORS = "max_errors";

    public static final String CONFIG_MAX_ERROR_RATIO = "max_error_ratio";
    public static final String CONFIG_ERROR_MODE = "error_mode";

    /**
     * Resolves an {@link ErrorPolicy} from the user's {@code WITH} options. Returns
     * {@code defaultPolicy} when none of {@link #CONFIG_ERROR_MODE},
     * {@link #CONFIG_MAX_ERRORS}, or {@link #CONFIG_MAX_ERROR_RATIO} are set.
     *
     * <p>Validation matches what {@code FileSourceFactory} applied historically: invalid
     * mode strings, non-numeric budgets, and {@code FAIL_FAST} combined with budget keys
     * are all rejected with {@link IllegalArgumentException}.
     */
    public static ErrorPolicy fromConfig(Map<String, Object> config, ErrorPolicy defaultPolicy) {
        if (config == null) {
            return defaultPolicy;
        }
        Object maxErrorsValue = config.get(CONFIG_MAX_ERRORS);
        Object maxErrorRatioValue = config.get(CONFIG_MAX_ERROR_RATIO);
        Object errorModeValue = config.get(CONFIG_ERROR_MODE);
        if (maxErrorsValue == null && maxErrorRatioValue == null && errorModeValue == null) {
            return defaultPolicy;
        }

        Mode mode = Mode.SKIP_ROW;
        if (errorModeValue != null) {
            String modeStr = errorModeValue.toString();
            try {
                mode = Mode.parse(modeStr);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_ERROR_MODE + "]: [" + errorModeValue + "]", e);
            }
            if (mode == null) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_ERROR_MODE + "]: [" + errorModeValue + "]");
            }
        }

        if (mode == Mode.FAIL_FAST) {
            if (maxErrorsValue != null || maxErrorRatioValue != null) {
                throw new IllegalArgumentException(
                    "["
                        + CONFIG_MAX_ERRORS
                        + "] and ["
                        + CONFIG_MAX_ERROR_RATIO
                        + "] cannot be used with ["
                        + CONFIG_ERROR_MODE
                        + "="
                        + mode
                        + "]; fail_fast always aborts on the first error"
                );
            }
            return STRICT;
        }

        long maxErrors;
        if (maxErrorsValue != null) {
            try {
                maxErrors = Long.parseLong(maxErrorsValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_MAX_ERRORS + "]: [" + maxErrorsValue + "]", e);
            }
        } else {
            maxErrors = Long.MAX_VALUE;
        }

        double maxErrorRatio = 0.0;
        if (maxErrorRatioValue != null) {
            try {
                maxErrorRatio = Double.parseDouble(maxErrorRatioValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for [" + CONFIG_MAX_ERROR_RATIO + "]: [" + maxErrorRatioValue + "]", e);
            }
        }

        boolean logErrors = maxErrors < Long.MAX_VALUE || maxErrorRatio > 0.0;
        return new ErrorPolicy(mode, maxErrors, maxErrorRatio, logErrors);
    }
}
