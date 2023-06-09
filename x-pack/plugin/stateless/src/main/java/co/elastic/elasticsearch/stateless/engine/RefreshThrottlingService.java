/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.Stateless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

public class RefreshThrottlingService extends AbstractLifecycleComponent {

    public static final String MEMORY_NODE_ATTR = Stateless.NAME + ".memory";
    public static final TimeValue BUDGET_INTERVAL = TimeValue.timeValueHours(1);
    public static final TimeValue THROTTLING_INTERVAL = TimeValue.timeValueSeconds(5);
    public static final ByteSizeValue NODE_BUDGET_HARDWARE_FACTOR = ByteSizeValue.ofGb(4);
    private static final Logger logger = LogManager.getLogger(RefreshThrottlingService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final RefreshNodeCreditManager systemIndicesCreditManager;
    private final RefreshNodeCreditManager regularIndicesCreditManager;

    public RefreshThrottlingService(Settings settings, ClusterService clusterService) {
        this.settings = settings;
        this.clusterService = clusterService;
        systemIndicesCreditManager = new RefreshNodeCreditManager(clusterService.threadPool()::relativeTimeInMillis, 1.0, "system");
        regularIndicesCreditManager = new RefreshNodeCreditManager(clusterService.threadPool()::relativeTimeInMillis, 1.0, "regular");
    }

    public RefreshThrottler.Factory createRefreshThrottlerFactory(IndexSettings indexSettings) {
        if (clusterService.localNode().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)
            && indexSettings.getIndexMetadata().isSystem() == false) {
            // TODO: once fast_refresh configuration is usable, return Noop only for those indices whose fast_refresh=true. For other
            // indices, return either the system or the regular node throttler depending on whether the index is a system index.
            if (indexSettings.getIndexMetadata().isSystem()) {
                return RefreshThrottler.Noop::new;
            }
            long maxCredit = (BUDGET_INTERVAL.seconds() / THROTTLING_INTERVAL.seconds());
            return refresh -> new RefreshBurstableThrottler(
                refresh,
                maxCredit,
                maxCredit,
                regularIndicesCreditManager,
                clusterService.threadPool()
            );
        }
        return RefreshThrottler.Noop::new;
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE)) {
            final long hardwareFactorBytes = NODE_BUDGET_HARDWARE_FACTOR.getBytes();
            clusterService.addListener(event -> {
                if (event.nodesChanged()) {
                    String localNodeMemStr = clusterService.localNode().getAttributes().get(MEMORY_NODE_ATTR);
                    if (localNodeMemStr == null || localNodeMemStr.isEmpty()) {
                        logger.debug("skipping updating refresh throttling multiplier since local node does not have a memory attribute");
                    } else {
                        final long localNodeBytes = Long.parseLong(localNodeMemStr);
                        long totalClusterBytes = 0;
                        long totalIndexNodesBytes = 0;

                        for (var node : event.state().nodes()) {
                            String nodeMemStr = node.getAttributes().get(MEMORY_NODE_ATTR);
                            if (nodeMemStr == null || nodeMemStr.isEmpty()) {
                                logger.debug(
                                    "skipping node [{}] when updating refresh throttling multiplier since it does not "
                                        + "have a memory attribute",
                                    node
                                );
                            } else {
                                long nodeBytes = Long.parseLong(nodeMemStr);
                                totalClusterBytes += nodeBytes;
                                if (node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
                                    totalIndexNodesBytes += nodeBytes;
                                }
                            }
                        }

                        assert totalIndexNodesBytes <= totalClusterBytes
                            : "index nodes bytes " + totalIndexNodesBytes + " more than total cluster bytes " + totalClusterBytes;

                        double newMultiplier = Math.max(
                            1.0,
                            1.0 * localNodeBytes / totalIndexNodesBytes * totalClusterBytes / hardwareFactorBytes
                        );
                        logger.debug("Updating multiplier for refresh throttling to [{}]", newMultiplier);
                        systemIndicesCreditManager.setMultiplier(newMultiplier);
                        regularIndicesCreditManager.setMultiplier(newMultiplier);
                    }
                }
            });
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

    // package private for testing
    RefreshNodeCreditManager getRegularIndicesCreditManager() {
        return regularIndicesCreditManager;
    }
}
