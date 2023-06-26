/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node.internal;

/**
 * Interface for termination handlers, which are called after Elasticsearch receives a signal from the OS indicating it should shut down
 * but before core services are stopped. These handlers may be called in any order or concurrently, so do not depend on ordering
 * guarantees and leave the system in a functioning state so that other handlers can complete.
 *
 * <p>Note that this class is mostly for plumbing - translating a low-level signal received by a node process into a higher-level set
 *  of operations. Logic to respond to planned changes in cluster membership should use Node Shutdown primitives instead, see
 *  {@link org.elasticsearch.plugins.ShutdownAwarePlugin} for lower-level plugin operations and
 *  {@link org.elasticsearch.cluster.metadata.NodesShutdownMetadata} for cluster state level operations.
 *  </p>
 */
public interface TerminationHandler {

    /**
     * The method which is called when the node is signalled to shut down. This method should block until the node is ready to shut down.
     */
    void handleTermination();
}
