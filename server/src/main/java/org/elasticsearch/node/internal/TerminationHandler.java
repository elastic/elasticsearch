/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node.internal;

/**
 * Interface for termination handlers, which are called after Elasticsearch receives a signal indicating it should shut down
 * but before core services are stopped. These handlers may be called in any order or concurrently, so do not depend on ordering
 * guarantees and leave the system in a functioning state so that other handlers can complete.
 */
public interface TerminationHandler {

    /**
     * Provides a name for this termination handler to allow easier problem isolation.
     *
     * @return The name of this termination handler.
     */
    String name();

    /**
     * The method which is called when the node is preparing to shut down. Do not block the thread - do any long-running work
     * asynchronously, and call the supplied Runnable when this handler has completed the work necessary to prepare the node
     * for graceful shutdown. Once all handlers have called their Runnables, the node will proceed with terminating core services.
     *
     * @param done This runnable should be invoked when this handler has finished preparing the node for shutdown.
     */
    void handleTermination(Runnable done);
}
