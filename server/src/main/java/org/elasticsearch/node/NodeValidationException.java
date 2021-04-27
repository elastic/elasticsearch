/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.common.transport.BoundTransportAddress;

import java.util.List;

/**
 * An exception thrown during node validation. Node validation runs immediately before a node
 * begins accepting network requests in
 * {@link Node#validateNodeBeforeAcceptingRequests(org.elasticsearch.bootstrap.BootstrapContext, BoundTransportAddress, List)}.
 * This exception is a checked exception that is declared as thrown from this method for the purpose of bubbling up to the user.
 */
public class NodeValidationException extends Exception {

    /**
     * Creates a node validation exception with the specified validation message to be displayed to
     * the user.
     *
     * @param message the message to display to the user
     */
    public NodeValidationException(final String message) {
        super(message);
    }

}
