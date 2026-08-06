/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.stream.Collectors;

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
        PhysicalPlan localPlan,
        String localExecutionPlanDescribe
    ) {}

    public static void logCoordinatorFailure(
        String sessionId,
        String clusterUuid,
        LogicalPlan parsed,
        LogicalPlan analyzed,
        LogicalPlan optimized,
        PhysicalPlan physical,
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
        String shards = context.shardIds().stream().map(ShardId::toString).collect(Collectors.joining(", "));
        try {
            var anonymized = PlanAnonymizer.forSubmission(context.clusterUuid())
                .anonymizeLocalCompute(context.localPlan(), context.localExecutionPlanDescribe());
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
