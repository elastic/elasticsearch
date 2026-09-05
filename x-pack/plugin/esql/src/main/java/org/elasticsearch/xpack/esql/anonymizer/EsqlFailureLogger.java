/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

/**
 * Structured, anonymized failure logging for ES|QL coordinator and data-node compute paths.
 */
public final class EsqlFailureLogger {

    private static final Logger LOGGER = LogManager.getLogger(EsqlFailureLogger.class);

    private EsqlFailureLogger() {}

    public record LocalComputeFailureContext(
        String sessionId,
        String clusterUuid,
        String clusterAlias,
        List<ShardId> shardIds,
        @Nullable PhysicalPlan localPlan,
        @Nullable String localExecutionPlanDescribe
    ) {}

    /**
     * Logs an anonymized coordinator-side failure. Safe to call unconditionally; guards internally on
     * log level, null {@code parsed}, and whether the status is ≥ 500.
     */
    public static void logCoordinatorFailure(
        String sessionId,
        String clusterUuid,
        @Nullable LogicalPlan parsed,
        @Nullable LogicalPlan analyzed,
        @Nullable LogicalPlan optimized,
        @Nullable PhysicalPlan physical,
        Exception err
    ) {
        if (LOGGER.isErrorEnabled() == false) {
            return;
        }
        if (parsed == null) {
            return;
        }
        if (shouldLogInternalServerError(err) == false) {
            return;
        }
        try {
            var anonymized = PlanAnonymizer.forSubmission(clusterUuid).anonymize(parsed, analyzed, optimized, physical);
            LOGGER.error(
                """
                    ES|QL query failed in session [{}]
                    failure:
                    {}
                    parsed:
                    {}
                    analyzed:
                    {}
                    optimized:
                    {}
                    physical:
                    {}
                    schema:
                    {}""".stripIndent(),
                sessionId,
                err.getMessage(),
                anonymized.parsed(),
                anonymized.analyzed(),
                anonymized.optimized(),
                anonymized.physical(),
                anonymized.schema(),
                err
            );
        } catch (Exception e) {
            LOGGER.warn("Plan anonymization failed for session [{}]", sessionId, e);
        }
    }

    /**
     * Logs an anonymized data-node compute failure. Safe to call unconditionally; guards internally on
     * log level, null {@code context.localPlan()}, and whether the status is ≥ 500.
     */
    public static void logLocalComputeFailure(LocalComputeFailureContext context, Exception err) {
        if (LOGGER.isErrorEnabled() == false) {
            return;
        }
        if (context.localPlan() == null) {
            return;
        }
        if (shouldLogInternalServerError(err) == false) {
            return;
        }
        try {
            var anonymizer = PlanAnonymizer.forSubmission(context.clusterUuid());
            String shards = anonymizer.anonymizeShardIds(context.shardIds());
            var anonymized = anonymizer.anonymizeLocalCompute(context.localPlan(), context.localExecutionPlanDescribe());
            LOGGER.error(
                """
                    ES|QL local compute failed in session [{}] cluster [{}] shards [{}]
                    failure:
                    {}
                    localPhysical:
                    {}
                    localExecution:
                    {}""".stripIndent(),
                context.sessionId(),
                context.clusterAlias(),
                shards,
                err.getMessage(),
                anonymized.physical(),
                anonymized.executionPlan(),
                err
            );
        } catch (Exception e) {
            LOGGER.warn("Plan anonymization failed for session [{}]", context.sessionId(), e);
        }
    }

    static boolean shouldLogInternalServerError(Exception err) {
        return ExceptionsHelper.status(err).getStatus() >= RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }
}
