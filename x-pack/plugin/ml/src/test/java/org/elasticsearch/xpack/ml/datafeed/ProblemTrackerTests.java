/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class ProblemTrackerTests extends ESTestCase {

    private AnomalyDetectionAuditor auditor;

    private ProblemTracker problemTracker;

    private static final long NUM_SEARCHES_IN_DAY = 24L;

    @Before
    public void setUpTests() {
        auditor = mock(AnomalyDetectionAuditor.class);
        problemTracker = new ProblemTracker(auditor, "foo", NUM_SEARCHES_IN_DAY);
    }

    public void testReportExtractionProblem() {
        problemTracker.reportExtractionProblem(createExtractionProblem("top level", "cause"));

        verify(auditor).error("foo", "Datafeed is encountering errors extracting data: cause");
        assertTrue(problemTracker.hasProblems());
    }

    public void testReportExtractionProblem_GivenSearchPhaseExecutionException() {
        SearchPhaseExecutionException searchPhaseExecutionException = new SearchPhaseExecutionException(
            "test-phase",
            "partial shards failure",
            new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchException("for the cause!")) }
        );

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, searchPhaseExecutionException));

        verify(auditor).error("foo", "Datafeed is encountering errors extracting data: for the cause!");
        assertTrue(problemTracker.hasProblems());
    }

    public void testReportAnalysisProblem() {
        problemTracker.reportAnalysisProblem(createAnalysisProblem("top level", "cause"));

        verify(auditor).error("foo", "Datafeed is encountering errors submitting data for analysis: cause");
        assertTrue(problemTracker.hasProblems());
    }

    public void testReportProblem_GivenSameProblemTwice() {
        problemTracker.reportExtractionProblem(createExtractionProblem("top level", "cause"));
        problemTracker.reportAnalysisProblem(createAnalysisProblem("top level", "cause"));

        verify(auditor, times(1)).error("foo", "Datafeed is encountering errors extracting data: cause");
        assertTrue(problemTracker.hasProblems());
    }

    public void testReportProblem_GivenSameProblemAfterFinishReport() {
        problemTracker.reportExtractionProblem(createExtractionProblem("top level", "cause"));
        problemTracker.finishReport();
        problemTracker.reportExtractionProblem(createExtractionProblem("top level", "cause"));

        verify(auditor, times(1)).error("foo", "Datafeed is encountering errors extracting data: cause");
        assertTrue(problemTracker.hasProblems());
    }

    public void testUpdateEmptyDataCount_GivenEmptyNineTimes() {
        for (int i = 0; i < 9; i++) {
            problemTracker.reportEmptyDataCount();
        }

        Mockito.verifyNoMoreInteractions(auditor);
    }

    public void testUpdateEmptyDataCount_GivenEmptyTenTimes() {
        for (int i = 0; i < 10; i++) {
            problemTracker.reportEmptyDataCount();
        }

        verify(auditor).warning("foo", "Datafeed has been retrieving no data for a while");
    }

    public void testUpdateEmptyDataCount_GivenEmptyElevenTimes() {
        for (int i = 0; i < 11; i++) {
            problemTracker.reportEmptyDataCount();
        }

        verify(auditor, times(1)).warning("foo", "Datafeed has been retrieving no data for a while");
    }

    public void testUpdateEmptyDataCount_GivenNonEmptyAfterNineEmpty() {
        for (int i = 0; i < 9; i++) {
            problemTracker.reportEmptyDataCount();
        }
        problemTracker.reportNonEmptyDataCount();

        Mockito.verifyNoMoreInteractions(auditor);
    }

    public void testUpdateEmptyDataCount_GivenNonEmptyAfterTenEmpty() {
        for (int i = 0; i < 10; i++) {
            problemTracker.reportEmptyDataCount();
        }
        problemTracker.reportNonEmptyDataCount();

        verify(auditor).warning("foo", "Datafeed has been retrieving no data for a while");
        verify(auditor).info("foo", "Datafeed has started retrieving data again");
    }

    public void testUpdateEmptyDataCount_DailyTrigger() {
        for (int i = 0; i < NUM_SEARCHES_IN_DAY; i++) {
            problemTracker.reportEmptyDataCount();
        }
        verify(auditor, times(2)).warning("foo", "Datafeed has been retrieving no data for a while");

        for (int i = 0; i < NUM_SEARCHES_IN_DAY; i++) {
            problemTracker.reportEmptyDataCount();
        }
        verify(auditor, times(3)).warning("foo", "Datafeed has been retrieving no data for a while");
    }

    public void testUpdateEmptyDataCount_NumSearchesInDayIsZero() {
        auditor = mock(AnomalyDetectionAuditor.class);
        problemTracker = new ProblemTracker(auditor, "foo", 0);

        problemTracker.reportEmptyDataCount();
        verify(auditor, times(1)).warning("foo", "Datafeed has been retrieving no data for a while");
    }

    public void testFinishReport_GivenNoProblems() {
        problemTracker.finishReport();

        assertFalse(problemTracker.hasProblems());
        Mockito.verifyNoMoreInteractions(auditor);
    }

    public void testFinishReport_GivenRecovery() {
        problemTracker.reportExtractionProblem(createExtractionProblem("top level", "bar"));
        problemTracker.finishReport();
        problemTracker.finishReport();

        verify(auditor).error("foo", "Datafeed is encountering errors extracting data: bar");
        verify(auditor).info("foo", "Datafeed has recovered data extraction and analysis");
        assertFalse(problemTracker.hasProblems());
    }

    private static final String PARENT_SEARCH_SHARDS_BREAKER_PREFIX =
        "[parent] Data too large, data for [indices:admin/search/search_shards] would be ";

    public void testParentCircuitBreakerExtractionProblemShouldAuditActionableGuidance() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[100/100b]", 100L);

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, parentCircuitBreaker));

        assertParentCircuitBreakerAudit();
        assertTrue(problemTracker.hasProblems());
    }

    public void testSearchPhaseParentCircuitBreakerShouldAuditActionableGuidance() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[100/100b]", 100L);
        SearchPhaseExecutionException searchPhaseExecutionException = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { new ShardSearchFailure(parentCircuitBreaker) }
        );

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, searchPhaseExecutionException));

        assertParentCircuitBreakerAudit();
    }

    public void testSearchPhaseParentCircuitBreakerCauseWithoutShardFailuresShouldAuditActionableGuidance() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[100/100b]", 100L);
        SearchPhaseExecutionException searchPhaseExecutionException = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            parentCircuitBreaker,
            ShardSearchFailure.EMPTY_ARRAY
        );

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, searchPhaseExecutionException));

        assertParentCircuitBreakerAudit();
    }

    public void testSearchPhaseParentCircuitBreakerNotFirstShardFailureShouldAuditActionableGuidance() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[200/200b]", 200L);
        SearchPhaseExecutionException searchPhaseExecutionException = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {
                new ShardSearchFailure(new ElasticsearchException("shard unavailable")),
                new ShardSearchFailure(parentCircuitBreaker) }
        );

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, searchPhaseExecutionException));

        assertParentCircuitBreakerAudit();
    }

    public void testNonParentCircuitBreakerExtractionProblemShouldPreserveOriginalMessage() {
        CircuitBreakingException requestCircuitBreaker = new CircuitBreakingException(
            "[request] Data too large, data for [search] would be [100/100b], which is larger than the limit of [90/90b]",
            CircuitBreaker.Durability.TRANSIENT
        );

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, requestCircuitBreaker));

        verify(auditor).error("foo", "Datafeed is encountering errors extracting data: " + requestCircuitBreaker.getMessage());
        assertThat(requestCircuitBreaker.getMessage(), not(containsString("usually transient")));
    }

    public void testCyclicCauseChainShouldTerminateWithoutParentCircuitBreaker() {
        RuntimeException cycleRoot = new RuntimeException("cycle-root");
        RuntimeException cycleTail = new CyclicCauseException("cycle-tail", cycleRoot);
        cycleRoot.initCause(cycleTail);

        assertNull(ProblemTracker.findParentCircuitBreaker(cycleRoot));
    }

    public void testCyclicCauseChainWithParentBreakerShouldStillFindParentCircuitBreaker() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[100/100b]", 100L);
        RuntimeException cycleTail = new CyclicCauseException("cycle-tail", parentCircuitBreaker);
        parentCircuitBreaker.initCause(cycleTail);

        assertSame(parentCircuitBreaker, ProblemTracker.findParentCircuitBreaker(parentCircuitBreaker));
    }

    public void testCyclicCauseChainWithParentBreakerShouldAuditActionableGuidance() {
        CircuitBreakingException parentCircuitBreaker = createParentCircuitBreaker("[100/100b]", 100L);
        RuntimeException cycleTail = new CyclicCauseException("cycle-tail", parentCircuitBreaker);
        parentCircuitBreaker.initCause(cycleTail);

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, parentCircuitBreaker));

        assertParentCircuitBreakerAudit();
    }

    public void testParentCircuitBreakerWithDifferentByteCountsShouldAuditOncePerEpisode() {
        CircuitBreakingException firstTrip = createParentCircuitBreaker("[100/100b]", 100L);
        CircuitBreakingException secondTrip = createParentCircuitBreaker("[200/200b]", 200L);

        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, firstTrip));
        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, secondTrip));

        verify(auditor, times(1)).error(eq("foo"), eq(expectedParentCircuitBreakerAudit(firstTrip.getMessage())));

        problemTracker.finishReport();
        problemTracker.finishReport();

        CircuitBreakingException thirdTrip = createParentCircuitBreaker("[300/300b]", 300L);
        problemTracker.reportExtractionProblem(new DatafeedJob.ExtractionProblemException(0L, thirdTrip));

        verify(auditor, times(1)).error(eq("foo"), eq(expectedParentCircuitBreakerAudit(firstTrip.getMessage())));
        verify(auditor, times(1)).error(eq("foo"), eq(expectedParentCircuitBreakerAudit(thirdTrip.getMessage())));
    }

    private void assertParentCircuitBreakerAudit() {
        verify(auditor).error(
            eq("foo"),
            argThat(
                allOf(
                    containsString("usually transient"),
                    containsString("needs no action"),
                    containsString("not advanced"),
                    containsString("retries it automatically"),
                    containsString("datafeed's"),
                    containsString("Data too large"),
                    containsString("search_shards")
                )
            )
        );
    }

    private static String expectedParentCircuitBreakerAudit(String circuitBreakerDetails) {
        return Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR,
            Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_PARENT_CIRCUIT_BREAKER, circuitBreakerDetails)
        );
    }

    private static CircuitBreakingException createParentCircuitBreaker(String wantedSize, long bytesWanted) {
        String message = PARENT_SEARCH_SHARDS_BREAKER_PREFIX + wantedSize + ", which is larger than the limit of [90/90b]";
        return new CircuitBreakingException(message, bytesWanted, 90L, CircuitBreaker.Durability.TRANSIENT);
    }

    private static DatafeedJob.ExtractionProblemException createExtractionProblem(String error, String cause) {
        Exception causeException = new RuntimeException(cause);
        Exception wrappedException = new TestWrappedException(error, causeException);
        return new DatafeedJob.ExtractionProblemException(0L, wrappedException);
    }

    private static DatafeedJob.AnalysisProblemException createAnalysisProblem(String error, String cause) {
        Exception causeException = new RuntimeException(cause);
        Exception wrappedException = new TestWrappedException(error, causeException);
        return new DatafeedJob.AnalysisProblemException(0L, false, wrappedException);
    }

    private static class TestWrappedException extends RuntimeException implements ElasticsearchWrapperException {

        TestWrappedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class CyclicCauseException extends RuntimeException {

        private final Throwable cycleCause;

        CyclicCauseException(String message, Throwable cycleCause) {
            super(message);
            this.cycleCause = cycleCause;
        }

        @Override
        public Throwable getCause() {
            return cycleCause;
        }
    }
}
