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

package co.elastic.elasticsearch.stateless.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EstimatedHeapUsageMonitor {

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageMonitor.class);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final RerouteService rerouteService;
    private volatile boolean thresholdEnabled;
    private volatile RatioValue estimatedHeapLowWatermark;
    private final AtomicReference<Set<String>> lastKnownNodeIdsExceedingLowWatermark = new AtomicReference<>(Set.of());

    public EstimatedHeapUsageMonitor(
        ClusterSettings clusterSettings,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.rerouteService = rerouteService;
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            newValue -> this.thresholdEnabled = newValue
        );
        clusterSettings.initializeAndWatch(
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            newValue -> this.estimatedHeapLowWatermark = newValue
        );
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        if (clusterStateSupplier.get().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (thresholdEnabled == false) {
            logger.debug("skipping monitor as the estimated heap usage threshold is disabled");
            return;
        }

        final var nodeIdsExceedingLowWatermark = clusterInfo.getEstimatedHeapUsages()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().estimatedUsageAsPercentage() > estimatedHeapLowWatermark.getAsPercent())
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());

        final var previousNodeIds = lastKnownNodeIdsExceedingLowWatermark.getAndSet(nodeIdsExceedingLowWatermark);
        if (nodeIdsExceedingLowWatermark.containsAll(previousNodeIds) == false) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    Strings.format(
                        "estimated heap usages dropped below the low watermark [%.2f] for nodes %s, triggering reroute",
                        estimatedHeapLowWatermark.getAsPercent(),
                        Sets.difference(previousNodeIds, nodeIdsExceedingLowWatermark)
                    )
                );
            }
            final String reason = "estimated heap usages drop below low watermark";
            rerouteService.reroute(
                reason,
                Priority.NORMAL,
                ActionListener.wrap(
                    ignored -> logger.trace("{} reroute successful", reason),
                    e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
                )
            );
        }
    }
}
