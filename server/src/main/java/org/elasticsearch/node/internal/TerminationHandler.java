/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
     * Whether the node is confirmed safe to shut down once {@link #handleTermination()} returns.
     */
    enum Outcome {
        /**
         * The cluster confirmed that the node's work, such as its shards, is safe to serve elsewhere. The node can then stop
         * accepting new work that a peer can retry elsewhere, and drain the work it already accepted.
         */
        READY,
        /**
         * Anything short of {@link #READY}: the confirmation failed, timed out or was never obtained. The node then only waits for
         * the work in flight, without turning new work away.
         */
        NOT_READY
    }

    /**
     * The method which is called when the node is signalled to shut down. This method should block until the node is ready to shut down,
     * or until it gives up waiting.
     *
     * @return {@link Outcome#READY} only if the node is confirmed safe to shut down, {@link Outcome#NOT_READY} otherwise. A handler
     *         that throws is treated as {@link Outcome#NOT_READY}.
     */
    Outcome handleTermination();

    /**
     * Block termination of this node.
     *
     * This method will be called during shutdown before any other shutdown actions occur, allowing the implementation to
     * block termination until it is safe to shut down.
     */
    default void blockTermination() {}
}
