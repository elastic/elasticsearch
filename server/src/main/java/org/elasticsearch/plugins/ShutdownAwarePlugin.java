/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;

import java.util.Collection;

/**
 * A {@link ShutdownAwarePlugin} is a plugin that can be made aware of a shutdown. It comprises two
 * parts, one part used for telling plugins that a set of nodes are going to be shut down
 * ({@link #signalShutdown(Collection)}), the other for retrieving the status of those plugins
 * as to whether it is safe to shut down ({@link #safeToShutdown(String, SingleNodeShutdownMetadata.Type)}
 */
public interface ShutdownAwarePlugin {

    /**
     * Whether the plugin is considered safe to shut down. This method is called when the status of
     * a shutdown is retrieved via the API, and it is only called on the master node.
     */
    boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType);

    /**
     * A trigger to notify the plugin that a shutdown for the nodes has been triggered. This method
     * will be called on every node for each cluster state, so it should return quickly.
     */
    void signalShutdown(Collection<String> shutdownNodeIds);
}
