/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

public class ConnectorInvalidStatusTransitionException extends Exception {

    /**
     * Constructs a {@link ConnectorInvalidStatusTransitionException} exception with a detailed message.
     *
     * @param current The current state of the {@link Connector}.
     * @param next The attempted next state of the {@link Connector}.
     */
    public ConnectorInvalidStatusTransitionException(ConnectorStatus current, ConnectorStatus next) {
        super(
            "Invalid transition attempt from ["
                + current
                + "] to ["
                + next
                + "]. Such a status transition is not supported by the Connector Protocol."
        );
    }
}
