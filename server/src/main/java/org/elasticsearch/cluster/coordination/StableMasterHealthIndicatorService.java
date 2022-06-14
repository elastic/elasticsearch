/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.UserAction;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

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
    private static final String HELP_URL = "https://ela.st/fix-master";

    private final StableMasterService stableMasterService;

    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);

    // Keys for the details map:
    private static final String DETAILS_CURRENT_MASTER = "current_master";
    private static final String DETAILS_RECENT_MASTERS = "recent_masters";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY = "exception_fetching_history";

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

    public StableMasterHealthIndicatorService(StableMasterService stableMasterService) {
        this.stableMasterService = stableMasterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public String helpURL() {
        return HELP_URL;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        StableMasterService.StableMasterResult stableMasterResult = stableMasterService.calculate(explain);
        return getHealthIndicatorResult(stableMasterResult, explain);
    }

    private HealthIndicatorResult getHealthIndicatorResult(StableMasterService.StableMasterResult stableMasterResult, boolean explain) {
        HealthStatus status = stableMasterResult.status();
        boolean statusIsGreen = status.equals(HealthStatus.GREEN);
        HealthIndicatorDetails details = getDetails(stableMasterResult.details(), explain);
        Collection<HealthIndicatorImpact> impacts = statusIsGreen ? List.of() : UNSTABLE_MASTER_IMPACTS;
        List<UserAction> userActions = statusIsGreen ? List.of() : getContactSupportUserActions(explain);
        return createIndicator(status, stableMasterResult.summary(), details, impacts, userActions);
    }

    private HealthIndicatorDetails getDetails(StableMasterService.StableMasterDetails stableMasterDetails, boolean explain) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            DiscoveryNode masterNode = stableMasterDetails.currentMaster();
            builder.object(DETAILS_CURRENT_MASTER, xContentBuilder -> {
                if (masterNode != null) {
                    builder.field("node_id", masterNode.getId());
                    builder.field("name", masterNode.getName());
                } else {
                    builder.nullField("node_id");
                    builder.nullField("name");
                }
            });
            List<DiscoveryNode> recentMasters = stableMasterDetails.recentMasters();
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
            String remoteHistoryExceptionMessage = stableMasterDetails.remoteExceptionMessage();
            if (remoteHistoryExceptionMessage != null) {
                builder.object(DETAILS_EXCEPTION_FETCHING_HISTORY, xContentBuilder -> {
                    builder.field("message", remoteHistoryExceptionMessage);
                    builder.field("stack_trace", stableMasterDetails.remoteExceptionStackTrace());
                });
            }
            return builder.endObject();
        };
    }

    /**
     * This method returns the only user action that is relevant when the master is unstable -- contact support.
     * @param explain If true, the returned list includes a UserAction to contact support, otherwise an empty list
     * @return a single UserAction instructing users to contact support.
     */
    private List<UserAction> getContactSupportUserActions(boolean explain) {
        if (explain) {
            UserAction.Definition contactSupport = new UserAction.Definition(
                "contact_support",
                "The Elasticsearch cluster does not have a stable master node. Please contact Elastic Support "
                    + "(https://support.elastic.co) to discuss available options.",
                null
            );
            UserAction userAction = new UserAction(contactSupport, null);
            return List.of(userAction);
        } else {
            return List.of();
        }
    }
}
