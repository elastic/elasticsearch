/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;

public class ConnectorSyncJobInvalidStatusTransitionException extends Exception {

    /**
     * Constructs a {@link ConnectorSyncJobInvalidStatusTransitionException} exception with a detailed message.
     *
     * @param current The current {@link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     * @param next The attempted next {@link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     */
    public ConnectorSyncJobInvalidStatusTransitionException(ConnectorSyncStatus current, ConnectorSyncStatus next) {
        super(
            "Invalid transition attempt from ["
                + current
                + "] to ["
                + next
                + "]. Such a "
                + ConnectorSyncStatus.class.getSimpleName()
                + " transition is not supported by the Connector Protocol."
        );
    }
}
