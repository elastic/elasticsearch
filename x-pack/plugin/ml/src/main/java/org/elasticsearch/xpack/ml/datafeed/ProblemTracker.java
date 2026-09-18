/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

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

    private static final String PARENT_CIRCUIT_BREAKER_DEDUP_KEY = "parent_circuit_breaker";

    private final AnomalyDetectionAuditor auditor;
    private final String jobId;

    private volatile boolean hasProblems;
    private volatile boolean hadProblems;
    private volatile String previousProblem;
    private volatile int consecutiveSameProblemCount;
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
        CircuitBreakingException parentCircuitBreaker = findParentCircuitBreaker(error);
        if (parentCircuitBreaker != null) {
            String problemMessage = Messages.getMessage(
                Messages.JOB_AUDIT_DATAFEED_PARENT_CIRCUIT_BREAKER,
                parentCircuitBreaker.getMessage()
            );
            reportProblem(Messages.JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR, problemMessage, PARENT_CIRCUIT_BREAKER_DEDUP_KEY);
        } else {
            reportProblem(
                Messages.JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR,
                ExceptionsHelper.findSearchExceptionRootCause(error).getMessage()
            );
        }
    }

    /**
     * Reports the problem if it is different than the last seen problem
     *
     * @param problemMessage the problem message
     */
    private void reportProblem(String template, String problemMessage) {
        reportProblem(template, problemMessage, problemMessage);
    }

    private void reportProblem(String template, String problemMessage, String dedupKey) {
        hasProblems = true;
        if (Objects.equals(previousProblem, dedupKey)) {
            // Same problem repeating: increment counter and re-audit periodically so a persistent
            // failure doesn't become permanently invisible after the first dedup suppression.
            consecutiveSameProblemCount++;
            if (consecutiveSameProblemCount % numberOfSearchesInADay == 0) {
                auditor.error(jobId, Messages.getMessage(template, problemMessage));
            }
        } else {
            consecutiveSameProblemCount = 1;
            previousProblem = dedupKey;
            auditor.error(jobId, Messages.getMessage(template, problemMessage));
        }
    }

    static CircuitBreakingException findParentCircuitBreaker(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> queue = new ArrayDeque<>();
        queue.add(error);
        while (queue.isEmpty() == false) {
            Throwable current = queue.removeFirst();
            if (seen.add(current) == false) {
                continue;
            }
            if (current instanceof CircuitBreakingException circuitBreakingException && isParentCircuitBreaker(circuitBreakingException)) {
                return circuitBreakingException;
            }
            if (current instanceof SearchPhaseExecutionException searchPhaseExecutionException) {
                for (ShardSearchFailure shardFailure : searchPhaseExecutionException.shardFailures()) {
                    if (shardFailure.getCause() != null) {
                        queue.add(shardFailure.getCause());
                    }
                }
            }
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return null;
    }

    private static boolean isParentCircuitBreaker(CircuitBreakingException circuitBreakingException) {
        String message = circuitBreakingException.getMessage();
        return message != null && message.startsWith("[" + CircuitBreaker.PARENT + "] ");
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
            previousProblem = null;
            consecutiveSameProblemCount = 0;
        }

        hadProblems = hasProblems;
        hasProblems = false;
    }
}
