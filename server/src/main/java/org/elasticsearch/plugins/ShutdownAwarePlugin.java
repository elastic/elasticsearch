/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ShutdownAwarePlugin} is a plugin that can be made aware of a shutdown. It comprises three
 * parts. One used for telling plugins that a set of nodes are going to be shut down
 * ({@link #signalShutdown(Collection)}). Another for retrieving the status of those plugins
 * as to whether it is safe to shut down ({@link #safeToShutdown(String, SingleNodeShutdownMetadata.Type)}).
 * And finally, a plugin can also name the actions whose in-flight tasks a node waits for when the
 * operating system asks it to shut down ({@link #getActionsToAwaitOnShutdown()}).
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

    /**
     * Returns the names of the actions whose in-flight tasks the node waits for when it shuts down gracefully in response to a signal
     * from the operating system, see {@link org.elasticsearch.node.ShutdownPrepareService}. Unlike the other methods of this interface,
     * this one is not tied to the Node Shutdown API: it is called once per node, when the node is built, and the result is used if that
     * node is later asked to shut down.
     * <p>
     * Without this, tasks that outlive the HTTP request that started them (e.g. async queries) or that run on behalf of a coordinator on
     * another node would be abandoned when the node closes. Tasks are matched by exact action name on the local task manager, so the
     * names must equal {@link org.elasticsearch.tasks.Task#getAction()}, including any suffix such as the one of the async tasks. The
     * wait is bounded by {@code node.maximum_shutdown_grace_period}; tasks still running when it elapses are not cancelled, the node just
     * proceeds with the shutdown.
     */
    default Collection<String> getActionsToAwaitOnShutdown() {
        return Collections.emptyList();
    }
}
