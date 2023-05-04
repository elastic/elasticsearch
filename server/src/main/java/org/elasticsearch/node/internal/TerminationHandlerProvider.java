/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node.internal;

import java.util.Collection;

/**
 * SPI service interface for providing hooks to handle graceful termination. This class is mostly plumbing and implementations should be
 * mostly boilerplate that passes the handlers along from the {@link org.elasticsearch.plugins.Plugin} class. See {@link TerminationHandler}
 * for the class that's actually invoked upon receiving a termination signal.
 */
public interface TerminationHandlerProvider {

    /**
     * Returns a list of {@link TerminationHandler} implementations which will be called when the node is about to shut down, but before
     * the core services are terminated.
     *
     * @return A list of {@link  TerminationHandler}s
     */
    Collection<TerminationHandler> handlers();
}
