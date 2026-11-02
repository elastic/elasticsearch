/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.utils.MlInvariantLogger;

import java.util.Arrays;
import java.util.List;

/** Logs an invariant event when a delete-by-query issued for one job could also match other jobs' documents. */
public final class JobScopedDeleteCheck {

    private static final Logger logger = LogManager.getLogger(JobScopedDeleteCheck.class);
    private static final MlInvariantLogger invariantLogger = new MlInvariantLogger();

    private JobScopedDeleteCheck() {}

    /** Logs and returns normally; never alters or blocks the request. */
    public static void logIfNotJobScoped(String jobId, DeleteByQueryRequest request) {
        if (isJobScoped(jobId, request)) {
            return;
        }
        String indices = String.join(",", request.indices());
        invariantLogger.violated(
            logger,
            MlInvariantLogger.Kind.DELETE_MISSING_JOB_ID_FILTER,
            jobId + '/' + indices,
            Strings.format("delete-by-query for job [%s] on indices [%s] has no [job_id] filter for that job", jobId, indices)
        );
    }

    private static boolean isJobScoped(String jobId, DeleteByQueryRequest request) {
        if (Strings.hasText(jobId) == false) {
            return false;
        }
        // The job's results read alias carries a job_id filter, so a query against only that alias is already scoped.
        String resultsAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String[] indices = request.indices();
        if (indices.length > 0 && Arrays.stream(indices).allMatch(resultsAlias::equals)) {
            return true;
        }
        return isScoped(jobId, request.getSearchRequest().source().query());
    }

    private static boolean isScoped(String jobId, QueryBuilder query) {
        if (query instanceof TermQueryBuilder term) {
            return Job.ID.getPreferredName().equals(term.fieldName()) && jobId.equals(term.value());
        }
        if (query instanceof TermsQueryBuilder terms) {
            return Job.ID.getPreferredName().equals(terms.fieldName()) && List.<Object>of(jobId).equals(terms.values());
        }
        if (query instanceof ConstantScoreQueryBuilder constantScore) {
            return isScoped(jobId, constantScore.innerQuery());
        }
        // Document ids of job state embed the job id.
        if (query instanceof IdsQueryBuilder ids) {
            return ids.ids().stream().allMatch(id -> id.contains(jobId));
        }
        if (query instanceof BoolQueryBuilder bool) {
            return anyScoped(jobId, bool.must()) || anyScoped(jobId, bool.filter()) || shouldClausesAllScoped(jobId, bool);
        }
        return false;
    }

    private static boolean anyScoped(String jobId, List<QueryBuilder> clauses) {
        return clauses.stream().anyMatch(clause -> isScoped(jobId, clause));
    }

    private static boolean shouldClausesAllScoped(String jobId, BoolQueryBuilder bool) {
        boolean shouldRequired = bool.minimumShouldMatch() != null || (bool.must().isEmpty() && bool.filter().isEmpty());
        return shouldRequired && bool.should().isEmpty() == false && bool.should().stream().allMatch(clause -> isScoped(jobId, clause));
    }
}
