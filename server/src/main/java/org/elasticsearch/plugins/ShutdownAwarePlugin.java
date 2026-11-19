/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ShutdownAwarePlugin} is a plugin that can be made aware of a shutdown. It comprises three
 * parts. One used for telling plugins that a set of nodes are going to be shut down
 * ({@link #signalShutdown(Collection)}). Another for retrieving the status of those plugins
 * as to whether it is safe to shut down ({@link #safeToShutdown(String, SingleNodeShutdownMetadata.Type)}).
 * And finally, a plugin can also name the actions whose in-flight tasks a node waits for when the
 * operating system asks it to shut down ({@link #getActionsToAwaitOnShutdown()}), and drain the work
 * it accepted that tasks alone do not track ({@link #startDrainOnShutdown()}).
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
     * <p>
     * When to wait depends on whether the node has a termination handler. Without one (stateful), the wait covers the tasks running
     * when the shutdown starts. With one (stateless), the wait starts once the handler and the other shutdown hooks have finished and
     * once the drains of {@link #startDrainOnShutdown()} have completed, and covers the tasks running at that point. The tasks that run
     * while the handler moves the node's shards away are not observed until then.
     */
    default Collection<String> getActionsToAwaitOnShutdown() {
        return Collections.emptyList();
    }

    /**
     * Starts draining the work this plugin accepted on this node and returns a listener that completes once that work has finished.
     * Called at most once, during a graceful shutdown in response to a signal from the operating system, and only on a node whose
     * termination handler confirmed that the node is safe to shut down, i.e. its shards can be served elsewhere. It is not called if the
     * handler did not confirm, or if {@code node.maximum_shutdown_grace_period} ran out before the handler and the other shutdown hooks
     * finished. Once called, the plugin may turn away new work that its peers can retry on another node.
     * <p>
     * Use it for work that {@link #getActionsToAwaitOnShutdown()} cannot see, such as state a peer set up on this node in a request
     * whose task has finished, and that a later request will use. The node waits for the returned listener within
     * {@code node.maximum_shutdown_grace_period}, then waits for the tasks of {@link #getActionsToAwaitOnShutdown()}. A listener that
     * completes exceptionally counts as completed.
     * <p>
     * The call must return promptly: it must not wait or do network I/O.
     */
    default SubscribableListener<Void> startDrainOnShutdown() {
        return SubscribableListener.nullSuccess();
    }
}
