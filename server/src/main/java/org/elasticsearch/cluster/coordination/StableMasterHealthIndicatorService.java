/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This indicator reports the health of master stability.
 * If we have had a master within the last 30 seconds, and that master has not changed more than 3 times in the last 30 minutes, then
 * this will report GREEN.
 * If we have had a master within the last 30 seconds, but that master has changed more than 3 times in the last 30 minutes (and that is
 * confirmed by checking with the last-known master), then this will report YELLOW.
 * If we have not had a master within the last 30 seconds, then this will will report RED with one exception. That exception is when:
 * (1) no node is elected master, (2) this node is not master eligible, (3) some node is master eligible, (4) we ask a master-eligible node
 * to run this indicator, and (5) it comes back with a result that is not RED.
 * Since this indicator needs to be able to run when there is no master at all, it does not depend on the dedicated health node (which
 * requires the existence of a master).
 */
public class StableMasterHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "master_is_stable";
    public static final String GET_HELP_GUIDE = "https://ela.st/getting-help";
    public static final Diagnosis CONTACT_SUPPORT_USER_ACTION = new Diagnosis(
        new Diagnosis.Definition(
            "contact_support",
            "The Elasticsearch cluster does not have a stable master node.",
            "Get help at " + GET_HELP_GUIDE,
            GET_HELP_GUIDE
        ),
        null
    );

    private final CoordinationDiagnosticsService coordinationDiagnosticsService;

    // Keys for the details map:
    private static final String DETAILS_CURRENT_MASTER = "current_master";
    private static final String DETAILS_RECENT_MASTERS = "recent_masters";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY = "exception_fetching_history";
    private static final String CLUSTER_FORMATION = "cluster_formation";
    private static final String CLUSTER_FORMATION_MESSAGE = "cluster_formation_message";

    // Impacts of having an unstable master:
    private static final String UNSTABLE_MASTER_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";
    private static final String UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM will not "
        + "work. The _cat APIs will not work.";
    private static final String UNSTABLE_MASTER_BACKUP_IMPACT = "Snapshot and restore will not work. Searchable snapshots cannot be "
        + "mounted.";

    /**
     * This is the list of the impacts to be reported when the master node is determined to be unstable.
     */
    private static final List<HealthIndicatorImpact> UNSTABLE_MASTER_IMPACTS = List.of(
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)),
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(3, UNSTABLE_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP))
    );

    public StableMasterHealthIndicatorService(CoordinationDiagnosticsService coordinationDiagnosticsService) {
        this.coordinationDiagnosticsService = coordinationDiagnosticsService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult = coordinationDiagnosticsService
            .diagnoseMasterStability(explain);
        return getHealthIndicatorResult(coordinationDiagnosticsResult, explain);
    }

    /**
     * Transforms a CoordinationDiagnosticsService.CoordinationDiagnosticsResult into a HealthIndicatorResult.
     * @param coordinationDiagnosticsResult The CoordinationDiagnosticsResult from the CoordinationDiagnosticsService to be transformed
     * @param explain If false, the details and user actions returned will be empty
     * @return The HealthIndicatorResult
     */
    // Non-private for testing
    HealthIndicatorResult getHealthIndicatorResult(
        CoordinationDiagnosticsService.CoordinationDiagnosticsResult coordinationDiagnosticsResult,
        boolean explain
    ) {
        HealthStatus status = HealthStatus.fromCoordinationDiagnosticsStatus(coordinationDiagnosticsResult.status());
        HealthIndicatorDetails details = getDetails(coordinationDiagnosticsResult.details(), explain);
        Collection<HealthIndicatorImpact> impacts = status.indicatesHealthProblem() ? UNSTABLE_MASTER_IMPACTS : List.of();
        List<Diagnosis> diagnosis = status.indicatesHealthProblem() ? getContactSupportUserActions(explain) : List.of();
        return createIndicator(status, coordinationDiagnosticsResult.summary(), details, impacts, diagnosis);
    }

    /**
     * Returns a HealthIndicatorDetails populated with information from the coordinationDiagnosticsDetails. If explain is false,
     * HealthIndicatorDetails.EMPTY will be returned. Otherwise the xContent of the returned HealthIndicatorDetails will potentially
     * include some of "current_master", "recent_masters", and "exception_fetching_history" top-level objects. The "current_master" field
     * will have "node_id" and "name" fields. The "recent_masters" field will be an array of objects, each containing "node_id" and
     * "name" fields. The "exception_fetching_history" field will contain "message" and "stack_trace" fields.
     * @param coordinationDiagnosticsDetails The CoordinationDiagnosticsDetails to transform into a HealthIndicatorDetails
     * @param explain If false, HealthIndicatorDetails.EMPTY will be returned
     * @return A HealthIndicatorDetails
     */
    private HealthIndicatorDetails getDetails(
        CoordinationDiagnosticsService.CoordinationDiagnosticsDetails coordinationDiagnosticsDetails,
        boolean explain
    ) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            DiscoveryNode masterNode = coordinationDiagnosticsDetails.currentMaster();
            builder.object(DETAILS_CURRENT_MASTER, xContentBuilder -> {
                if (masterNode != null) {
                    builder.field("node_id", masterNode.getId());
                    builder.field("name", masterNode.getName());
                } else {
                    builder.nullField("node_id");
                    builder.nullField("name");
                }
            });
            List<DiscoveryNode> recentMasters = coordinationDiagnosticsDetails.recentMasters();
            if (recentMasters != null) {
                builder.array(DETAILS_RECENT_MASTERS, arrayXContentBuilder -> {
                    for (DiscoveryNode recentMaster : recentMasters) {
                        if (recentMaster != null) {
                            builder.startObject();
                            builder.field("node_id", recentMaster.getId());
                            builder.field("name", recentMaster.getName());
                            builder.endObject();
                        }
                    }
                });
            }
            String remoteHistoryExceptionMessage = coordinationDiagnosticsDetails.remoteExceptionMessage();
            if (remoteHistoryExceptionMessage != null) {
                builder.object(DETAILS_EXCEPTION_FETCHING_HISTORY, xContentBuilder -> {
                    builder.field("message", remoteHistoryExceptionMessage);
                    builder.field("stack_trace", coordinationDiagnosticsDetails.remoteExceptionStackTrace());
                });
            }
            if (coordinationDiagnosticsDetails.nodeToClusterFormationDescriptionMap() != null) {
                builder.field(
                    CLUSTER_FORMATION,
                    coordinationDiagnosticsDetails.nodeToClusterFormationDescriptionMap()
                        .entrySet()
                        .stream()
                        .map(entry -> Map.of("node_id", entry.getKey(), CLUSTER_FORMATION_MESSAGE, entry.getValue()))
                        .toList()
                );
            }
            return builder.endObject();
        };
    }

    /**
     * This method returns the only user action that is relevant when the master is unstable -- contact support.
     * @param explain If true, the returned list includes a UserAction to contact support, otherwise an empty list
     * @return a single UserAction instructing users to contact support.
     */
    private List<Diagnosis> getContactSupportUserActions(boolean explain) {
        if (explain) {
            return List.of(CONTACT_SUPPORT_USER_ACTION);
        } else {
            return List.of();
        }
    }
}
