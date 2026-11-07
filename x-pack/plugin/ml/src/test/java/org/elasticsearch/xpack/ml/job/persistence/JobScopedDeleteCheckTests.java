/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Level;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;

import java.util.regex.Pattern;

public class JobScopedDeleteCheckTests extends ESTestCase {

    private static final String SHARED_INDEX = ".ml-anomalies-shared";

    private final String jobId = "job-" + randomAlphaOfLength(8);

    private DeleteByQueryRequest request(QueryBuilder query, String... indices) {
        return new DeleteByQueryRequest(indices).setQuery(query);
    }

    private void assertViolation(String checkedJobId, DeleteByQueryRequest request) {
        MockLog.assertThatLogger(
            () -> JobScopedDeleteCheck.logIfNotJobScoped(checkedJobId, request),
            JobScopedDeleteCheck.class,
            new MockLog.PatternSeenEventExpectation(
                "violation",
                JobScopedDeleteCheck.class.getCanonicalName(),
                Level.ERROR,
                Pattern.quote("[ml-invariant:delete_missing_job_id_filter]") + ".*"
            )
        );
    }

    private void assertSilent(String checkedJobId, DeleteByQueryRequest request) {
        assertNoMissingJobScopeEvent(() -> JobScopedDeleteCheck.logIfNotJobScoped(checkedJobId, request));
    }

    /** Runs {@code action} and fails if it logs a missing-job-scope invariant event at ERROR or WARN. */
    public static void assertNoMissingJobScopeEvent(Runnable action) {
        String loggerName = JobScopedDeleteCheck.class.getCanonicalName();
        MockLog.assertThatLogger(
            action,
            JobScopedDeleteCheck.class,
            new MockLog.PatternNotSeenEventExpectation("no error event", loggerName, Level.ERROR, ".*"),
            new MockLog.PatternNotSeenEventExpectation("no warn event", loggerName, Level.WARN, ".*")
        );
    }

    public void testQueryWithoutJobIdTermShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.rangeQuery("timestamp").lt(1000), SHARED_INDEX));
    }

    public void testBlankJobIdShouldLogViolation() {
        String blank = randomFrom("", "  ");
        assertViolation(blank, request(QueryBuilders.termQuery("job_id", blank), ".ml-anomalies-" + randomAlphaOfLength(8)));
    }

    public void testTermForAnotherJobShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.termQuery("job_id", jobId + "-other"), SHARED_INDEX));
    }

    public void testTermOnAnotherFieldShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.termQuery("result_type", jobId), SHARED_INDEX));
    }

    public void testNoIndicesAndNoJobIdTermShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.matchAllQuery()));
    }

    public void testResultsAliasMixedWithOtherIndexAndNoJobIdTermShouldLogViolation() {
        String alias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        assertViolation(jobId, request(QueryBuilders.matchAllQuery(), alias, SHARED_INDEX));
    }

    public void testShouldClauseWithoutJobIdTermShouldLogViolation() {
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("job_id", jobId))
            .should(QueryBuilders.termQuery("result_type", "bucket"))
            .minimumShouldMatch(1);
        assertViolation(jobId, request(query, SHARED_INDEX));
    }

    public void testIdsQueryWithIdOfAnotherJobShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.idsQuery().addIds(jobId + "_quantiles", "someone-else_quantiles"), SHARED_INDEX));
    }

    public void testJobIdTermInFilterShouldBeSilent() {
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("job_id", jobId))
            .filter(QueryBuilders.rangeQuery("timestamp").lt(1000));
        assertSilent(jobId, request(query, SHARED_INDEX));
    }

    public void testTermsOnJobIdWithOnlyThisJobShouldBeSilent() {
        assertSilent(jobId, request(QueryBuilders.termsQuery("job_id", jobId), SHARED_INDEX));
    }

    public void testTermsOnJobIdWithAnotherJobShouldLogViolation() {
        assertViolation(jobId, request(QueryBuilders.termsQuery("job_id", jobId, jobId + "-other"), SHARED_INDEX));
    }

    public void testJobIdTermInMustShouldBeSilent() {
        assertSilent(jobId, request(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("job_id", jobId)), SHARED_INDEX));
    }

    public void testJobIdTermUnderConstantScoreShouldBeSilent() {
        assertSilent(jobId, request(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("job_id", jobId)), SHARED_INDEX));
    }

    public void testJobIdTermNestedInFilteredBoolShouldBeSilent() {
        QueryBuilder query = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("job_id", jobId)))
        );
        assertSilent(jobId, request(query, SHARED_INDEX));
    }

    public void testEveryShouldClauseCarriesJobIdTermShouldBeSilent() {
        QueryBuilder query = QueryBuilders.boolQuery()
            .minimumShouldMatch(1)
            .should(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("job_id", jobId)).must(QueryBuilders.termQuery("forecast_id", "a"))
            )
            .should(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("job_id", jobId)).must(QueryBuilders.termQuery("forecast_id", "b"))
            );
        assertSilent(jobId, request(QueryBuilders.boolQuery().filter(query), SHARED_INDEX));
    }

    public void testIdsQueryEmbeddingJobIdShouldBeSilent() {
        QueryBuilder query = QueryBuilders.idsQuery()
            .addIds(jobId + "_model_snapshot_1", jobId + "_model_state_1#1", "annotation_for_" + jobId + "_model_snapshot_1");
        assertSilent(jobId, request(query, ".ml-state-*", ".ml-annotations-read"));
    }

    public void testResultsAliasOnlyWithoutJobIdTermShouldBeSilent() {
        String alias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        assertSilent(jobId, request(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("is_interim", true)), alias));
    }
}
