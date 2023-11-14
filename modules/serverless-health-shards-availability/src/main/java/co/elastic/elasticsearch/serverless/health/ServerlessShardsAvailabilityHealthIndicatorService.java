/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.serverless.health;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedIndices;

public class ServerlessShardsAvailabilityHealthIndicatorService extends ShardsAvailabilityHealthIndicatorService {
    public static final String ALL_REPLICAS_UNASSIGNED_IMPACT_ID = "all_replicas_unassigned";

    public ServerlessShardsAvailabilityHealthIndicatorService(
        ClusterService clusterService,
        AllocationService allocationService,
        SystemIndices systemIndices
    ) {
        super(clusterService, allocationService, systemIndices);
    }

    @Override
    public ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus createNewStatus(Metadata metadata) {
        return new ServerlessShardAllocationStatus(metadata);
    }

    public class ServerlessShardAllocationStatus extends ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus {

        ServerlessShardAllocationStatus(Metadata clusterMetadata) {
            super(clusterMetadata);
        }

        /**
         * Overrides the existing status to indicate that the cluster is red when all replicas for a shard are not assigned
         */
        @Override
        public HealthStatus getStatus() {
            if (primaries.areAllAvailable() == false || primaries.searchableSnapshotsState.getRedSearchableSnapshots().isEmpty() == false) {
                return RED;
            } else if (replicas.areAllAvailable() == false) {
                if (replicas.doAnyIndicesHaveAllUnavailable()) {
                    // There are some indices where *all* indices are unavailable
                    return RED;
                } else {
                    return YELLOW;
                }
            } else {
                return GREEN;
            }
        }

        @Override
        public List<HealthIndicatorImpact> getImpacts() {
            List<HealthIndicatorImpact> impacts = new ArrayList<>(super.getImpacts());
            if (replicas.doAnyIndicesHaveAllUnavailable()) {
                String impactDescription = String.format(
                    Locale.ROOT,
                    "Not all data is searchable. No searchable copies of the data exist on %d %s [%s].",
                    replicas.indicesWithAllShardsUnavailable.size(),
                    replicas.indicesWithAllShardsUnavailable.size() == 1 ? "index" : "indices",
                    getTruncatedIndices(replicas.indicesWithAllShardsUnavailable, clusterMetadata)
                );
                impacts.add(
                    new HealthIndicatorImpact(NAME, ALL_REPLICAS_UNASSIGNED_IMPACT_ID, 1, impactDescription, List.of(ImpactArea.SEARCH))
                );
                if (replicas.indicesWithUnavailableShards.equals(replicas.indicesWithAllShardsUnavailable)) {
                    // Remove the other replica message, because all indices are already covered by the impact added above
                    impacts.removeIf(indicator -> indicator.id().equals(REPLICA_UNASSIGNED_IMPACT_ID));
                }
            }
            return impacts;
        }
    }

    @Override
    public List<Diagnosis.Definition> checkDataTierRelatedIssues(
        IndexMetadata indexMetadata,
        List<NodeAllocationResult> nodeAllocationResults,
        ClusterState clusterState
    ) {
        // TODO: Change this so that we don't return a red herring about
        // "oh, add more data_hot nodes" on Serverless, which doesn't make sense anyway.
        return super.checkDataTierRelatedIssues(indexMetadata, nodeAllocationResults, clusterState);
    }
}
