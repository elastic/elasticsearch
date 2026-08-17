/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * Runtime helper for Painless allocation tracking. The generated script class's {@link PainlessScript#$checkAllocBytes(long)}
 * override only charges the per-instance running total and passes the numbers to {@link #checkAllocation}, which owns every
 * decision about whether to warn, to fail the script, or to do nothing. Keeping the thresholds and their comparisons here
 * rather than in emitted bytecode means the policy reads as ordinary Java and the generated method stays a single call. Only
 * referenced when tracking is enabled (a positive limit or warning threshold).
 */
public final class AllocationGuard {

    private static final Logger logger = LogManager.getLogger(AllocationGuard.class);

    /** Cap on how much script source a threshold warning logs; see {@link #abbreviateSource}. */
    static final int MAX_LOGGED_SOURCE_LENGTH = 256;

    private AllocationGuard() {}

    /**
     * Clamps an {@code @allocates} estimator's result to {@code [0, Long.MAX_VALUE / 2]} before it is charged: a
     * negative result (an estimator bug) must not credit the running total, and a huge one must trip any configurable limit
     * without overflowing it (so estimators may return {@code Long.MAX_VALUE} for "definitely over"). Estimators must not
     * throw; a thrown exception propagates and fails the script.
     */
    public static long sanitizeEstimate(long estimatedBytes) {
        return Math.clamp(estimatedBytes, 0L, Long.MAX_VALUE / 2);
    }

    /**
     * Charges a {@code def}-dispatched {@code +} before it runs, but only when it is actually a string concat (an operand is a
     * {@link String}, per {@link DefMath}'s rule) — so numeric {@code def + def} rebox is left untracked by design. The estimate
     * reuses the statically-typed concat bound (see {@link AllocSizes#stringConcatOperandBytes}). Caller boxes primitive
     * operands so both arrive as {@link Object}; emitted only when tracking is enabled.
     */
    public static void checkDefConcatAlloc(PainlessScript script, Object left, Object right) {
        if (left instanceof String || right instanceof String) {
            script.$checkAllocBytes(
                AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + AllocSizes.stringConcatOperandBytes(left) + AllocSizes.stringConcatOperandBytes(
                    right
                )
            );
        }
    }

    /**
     * Decides what a charged allocation requires: a once-per-execution warning, an error that fails the script, both, or
     * nothing. This is the single place either threshold is compared, so the generated {@code $checkAllocBytes} override only
     * has to charge the running total and hand the numbers over — see
     * {@code DefaultIRTreeToASMBytesPhase#writeCheckAllocBytesMethod}.
     * <p>
     * Both thresholds use the {@code -1} sentinel for "off", so a warning-only, enforcement-only, both, or neither
     * configuration is expressed by the arguments rather than by which bytecode was emitted. Kept to two comparisons on the
     * common path, with the breach bodies in separate methods, so it stays small enough to inline into the caller.
     *
     * @param script the charging script, for its name and source on the warning path
     * @param scriptContextName script context, for breach messages and the metric attribute
     * @param attemptedBytes size of the allocation just charged
     * @param totalBytes running total after charging it
     * @param alreadyWarned whether this execution has already reported the warning threshold
     * @param warnBytes the per-context warning threshold, or {@code -1} when not warning
     * @param limitBytes the per-context limit, or {@code -1} when not enforcing
     * @return the new latch value, which the caller stores back to {@code $allocWarned}
     */
    public static boolean checkAllocation(
        PainlessScript script,
        String scriptContextName,
        long attemptedBytes,
        long totalBytes,
        boolean alreadyWarned,
        long warnBytes,
        long limitBytes
    ) {
        boolean warned = alreadyWarned;

        if (warnBytes > 0L && totalBytes > warnBytes && alreadyWarned == false) {
            warned = true;
            allocationWarnThresholdExceeded(script, scriptContextName, attemptedBytes, totalBytes, warnBytes);
        }

        // Checked after the warning so an allocation crossing both is reported before the error is raised. Never returns.
        if (limitBytes > 0L && totalBytes > limitBytes) {
            allocationLimitExceeded(scriptContextName, attemptedBytes, totalBytes, limitBytes);
        }

        return warned;
    }

    /**
     * Logs a {@code WARN} and returns normally — crossing the warning threshold never fails the script. Called at most once
     * per execution ({@link #checkAllocation} latches on {@code $allocWarned}), since the total stays above the threshold for
     * every later allocation and a hot script would otherwise log once per document. Unlike
     * {@link #allocationLimitExceeded} this names the script, as nothing follows it to identify the culprit.
     *
     * @param script the breaching script, for its name and source
     * @param scriptContextName script context, for the message and the metric attribute
     * @param attemptedBytes size of the allocation that crossed the threshold
     * @param totalBytes running total after charging it
     * @param warnBytes the per-context warning threshold
     */
    static void allocationWarnThresholdExceeded(
        PainlessScript script,
        String scriptContextName,
        long attemptedBytes,
        long totalBytes,
        long warnBytes
    ) {
        logger.warn(
            "Painless script [{}] in context [{}] exceeded its allocation warning threshold: allocation of [{}] bytes brings "
                + "running total to [{}] bytes (warning threshold [{}] bytes); this is reported once per execution and does "
                + "not fail the script. Source: [{}]",
            script.getName(),
            scriptContextName,
            attemptedBytes,
            totalBytes,
            warnBytes,
            abbreviateSource(script.getSource())
        );
        AllocationMetrics.getInstance().recordWarnExceeded(scriptContextName);
    }

    /**
     * Truncates a script's source for logging. The source is what makes a warning actionable, but a stored script can be
     * arbitrarily long and this line repeats once per execution, so it is capped rather than logged whole.
     */
    static String abbreviateSource(String source) {
        if (source == null) {
            return "";
        }
        if (source.length() <= MAX_LOGGED_SOURCE_LENGTH) {
            return source;
        }
        return source.substring(0, MAX_LOGGED_SOURCE_LENGTH) + "... (truncated from " + source.length() + " chars)";
    }

    /**
     * Logs a {@code WARN} and throws a {@link PainlessError} describing an allocation that pushed a script over its limit.
     * {@link PainlessError} is an {@link Error}, so it cannot be caught from Painless source. Never returns normally. The
     * specific allocation that crossed the limit is not reported: it is whichever happened to tip the running total, not
     * necessarily the dominant cost, so naming it would mislead more than help. Unlike the warning path this omits the script
     * name and source, since the {@link PainlessError} it throws reaches the caller with both attached.
     *
     * @param scriptContextName script context, for the message and the metric attribute
     * @param attemptedBytes the size of the allocation that tripped the limit
     * @param totalBytes the running total after charging the allocation
     * @param limitBytes the per-context limit
     */
    static void allocationLimitExceeded(String scriptContextName, long attemptedBytes, long totalBytes, long limitBytes) {
        logger.warn(
            "Painless script in context [{}] exceeded its allocation limit: allocation of [{}] bytes brings running total to "
                + "[{}] bytes (limit [{}] bytes)",
            scriptContextName,
            attemptedBytes,
            totalBytes,
            limitBytes
        );
        AllocationMetrics.getInstance().recordLimitExceeded(scriptContextName);
        throw new PainlessError(
            "script allocation limit exceeded: allocation of ["
                + attemptedBytes
                + "] bytes brings the running total to ["
                + totalBytes
                + "] bytes, over the limit of ["
                + limitBytes
                + "] bytes"
        );
    }
}
