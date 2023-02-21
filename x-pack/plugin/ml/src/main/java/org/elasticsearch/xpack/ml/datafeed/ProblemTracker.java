/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Objects;

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

    private final AnomalyDetectionAuditor auditor;
    private final String jobId;

    private volatile boolean hasProblems;
    private volatile boolean hadProblems;
    private volatile String previousProblem;
    private volatile int emptyDataCount;
    private final long numberOfSearchesInADay;

    ProblemTracker(AnomalyDetectionAuditor auditor, String jobId, long numberOfSearchesInADay) {
        this.auditor = Objects.requireNonNull(auditor);
        this.jobId = Objects.requireNonNull(jobId);
        this.numberOfSearchesInADay = Math.max(numberOfSearchesInADay, 1);
    }

    /**
     * Reports as analysis problem if it is different than the last seen problem
     *
     * @param error the exception
     */
    public void reportAnalysisProblem(DatafeedJob.AnalysisProblemException error) {
        reportProblem(Messages.JOB_AUDIT_DATAFEED_DATA_ANALYSIS_ERROR, ExceptionsHelper.unwrapCause(error).getMessage());
    }

    /**
     * Reports as extraction problem if it is different than the last seen problem
     *
     * @param error the exception
     */
    public void reportExtractionProblem(DatafeedJob.ExtractionProblemException error) {
        reportProblem(Messages.JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR, ExceptionsHelper.findSearchExceptionRootCause(error).getMessage());
    }

    /**
     * Reports the problem if it is different than the last seen problem
     *
     * @param problemMessage the problem message
     */
    private void reportProblem(String template, String problemMessage) {
        hasProblems = true;
        if (Objects.equals(previousProblem, problemMessage) == false) {
            previousProblem = problemMessage;
            auditor.error(jobId, Messages.getMessage(template, problemMessage));
        }
    }

    /**
     * Updates the tracking of empty data cycles. If the number of consecutive empty data
     * cycles reaches {@code EMPTY_DATA_WARN_COUNT} or the 24 hours of empty data counts
     * have passed a warning is reported.
     */
    public int reportEmptyDataCount() {
        if (++emptyDataCount == EMPTY_DATA_WARN_COUNT || (emptyDataCount % numberOfSearchesInADay) == 0) {
            auditor.warning(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_NO_DATA));
        }
        return emptyDataCount;
    }

    public void reportNonEmptyDataCount() {
        if (emptyDataCount >= EMPTY_DATA_WARN_COUNT) {
            auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_DATA_SEEN_AGAIN));
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
        if (hasProblems == false && hadProblems) {
            auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_RECOVERED));
        }

        hadProblems = hasProblems;
        hasProblems = false;
    }
}
