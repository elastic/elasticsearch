/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node.internal;

/**
 * SPI service interface for providing hooks to handle graceful termination. This class is mostly plumbing and implementations should be
 * mostly boilerplate that passes the handlers along from the {@link org.elasticsearch.plugins.Plugin} class. See {@link TerminationHandler}
 * for the class that's actually invoked upon receiving a termination signal.
 *
 * <p>Note that this class is mostly for plumbing - translating a low-level signal received by a node process into a higher-level set
 * of operations. Logic to respond to planned changes in cluster membership should use Node Shutdown primitives instead, see
 * {@link org.elasticsearch.plugins.ShutdownAwarePlugin} for lower-level plugin operations and
 * {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata} for cluster state level operations.
 * </p>
 */
public interface TerminationHandlerProvider {

    /**
     * Returns a {@link TerminationHandler} implementation which will be invoked when the node is about to shut down, but before
     * the core services are terminated.
     *
     * @return A {@link  TerminationHandler}s
     */
    TerminationHandler handler();
}
