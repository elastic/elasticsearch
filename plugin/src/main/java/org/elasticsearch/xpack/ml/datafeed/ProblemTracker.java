/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * <p>
 * Keeps track of problems the datafeed encounters and audits
 * messages appropriately.
 * </p>
 * <p>
 * The {@code ProblemTracker} is expected to interact with multiple
 * threads (lookback executor, real-time executor). However, each
 * thread will be accessing in a sequential manner therefore we
 * only need to ensure correct visibility.
 * </p>
 */
class ProblemTracker {

    private static final int EMPTY_DATA_WARN_COUNT = 10;

    private final Supplier<Auditor> auditor;

    private volatile boolean hasProblems;
    private volatile boolean hadProblems;
    private volatile String previousProblem;

    private volatile int emptyDataCount;

    ProblemTracker(Supplier<Auditor> auditor) {
        this.auditor = Objects.requireNonNull(auditor);
    }

    /**
     * Reports as analysis problem if it is different than the last seen problem
     *
     * @param problemMessage the problem message
     */
    public void reportAnalysisProblem(String problemMessage) {
        reportProblem(Messages.JOB_AUDIT_DATAFEED_DATA_ANALYSIS_ERROR, problemMessage);
    }

    /**
     * Reports as extraction problem if it is different than the last seen problem
     *
     * @param problemMessage the problem message
     */
    public void reportExtractionProblem(String problemMessage) {
        reportProblem(Messages.JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR, problemMessage);
    }

    /**
     * Reports the problem if it is different than the last seen problem
     *
     * @param problemMessage the problem message
     */
    private void reportProblem(String template, String problemMessage) {
        hasProblems = true;
        if (!Objects.equals(previousProblem, problemMessage)) {
            previousProblem = problemMessage;
            auditor.get().error(Messages.getMessage(template, problemMessage));
        }
    }

    /**
     * Updates the tracking of empty data cycles. If the number of consecutive empty data
     * cycles reaches {@code EMPTY_DATA_WARN_COUNT}, a warning is reported.
     */
    public void reportEmptyDataCount() {
        if (emptyDataCount < EMPTY_DATA_WARN_COUNT) {
            emptyDataCount++;
            if (emptyDataCount == EMPTY_DATA_WARN_COUNT) {
                auditor.get().warning(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_NO_DATA));
            }
        }
    }

    public void reportNoneEmptyCount() {
        if (emptyDataCount >= EMPTY_DATA_WARN_COUNT) {
            auditor.get().info(Messages.getMessage(Messages.JOB_AUDIR_DATAFEED_DATA_SEEN_AGAIN));
        }
        emptyDataCount = 0;
    }

    public boolean hasProblems() {
        return hasProblems;
    }

    /**
     * Issues a recovery message if appropriate and prepares for next report
     */
    public void finishReport() {
        if (!hasProblems && hadProblems) {
            auditor.get().info(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_RECOVERED));
        }

        hadProblems = hasProblems;
        hasProblems = false;
    }
}
