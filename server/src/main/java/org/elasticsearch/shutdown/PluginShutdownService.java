/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.ShutdownAwarePlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * The {@link PluginShutdownService} is used for the node shutdown infrastructure to signal to
 * plugins that a shutdown is occurring, and to check whether it is safe to shut down.
 */
public class PluginShutdownService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PluginShutdownService.class);
    public List<ShutdownAwarePlugin> plugins;

    public PluginShutdownService(@Nullable List<ShutdownAwarePlugin> plugins) {
        this.plugins = plugins == null ? Collections.emptyList() : plugins;
    }

    /**
     * Return all nodes shutting down from the given cluster state
     */
    public static Set<String> shutdownNodes(final ClusterState clusterState) {
        return clusterState.metadata().nodeShutdowns().getAllNodeIds();
    }

    /**
     * Return all nodes shutting down with the given shutdown types from the given cluster state
     */
    public static Set<String> shutdownTypeNodes(final ClusterState clusterState, final SingleNodeShutdownMetadata.Type... shutdownTypes) {
        Set<SingleNodeShutdownMetadata.Type> types = Arrays.stream(shutdownTypes).collect(toSet());
        return clusterState.metadata()
            .nodeShutdowns()
            .getAll()
            .entrySet()
            .stream()
            .filter(e -> types.contains(e.getValue().getType()))
            .map(Map.Entry::getKey)
            .collect(toSet());
    }

    /**
     * Check with registered plugins whether the shutdown is safe for the given node id and type
     */
    public boolean readyToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        // TODO: consider adding debugging information (a message about why not?)
        // TODO: consider adding more fine-grained status rather than true/false
        for (ShutdownAwarePlugin plugin : plugins) {
            try {
                if (plugin.safeToShutdown(nodeId, shutdownType) == false) {
                    logger.trace("shutdown aware plugin [{}] is not yet ready for shutdown", plugin);
                    return false;
                }
            } catch (Exception e) {
                logger.warn("uncaught exception when retrieving whether plugin is ready for node shutdown", e);
            }
        }
        return true;
    }

    /**
     * Signal to plugins the nodes that are currently shutting down
     */
    public void signalShutdown(final ClusterState state) {
        Set<String> shutdownNodes = shutdownNodes(state);
        for (ShutdownAwarePlugin plugin : plugins) {
            try {
                plugin.signalShutdown(shutdownNodes);
            } catch (Exception e) {
                logger.warn(() -> "uncaught exception when notifying plugins of nodes " + shutdownNodes + " shutdown", e);
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        signalShutdown(event.state());
    }
}
