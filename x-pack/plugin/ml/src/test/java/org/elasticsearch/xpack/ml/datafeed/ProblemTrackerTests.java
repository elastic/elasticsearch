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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
}
