/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link ConnectorSyncJobStateMachine} class manages state transitions for instances of {@link ConnectorSyncJob}
 * in accordance with the <a href="https://github.com/elastic/connectors/blob/main/docs/CONNECTOR_PROTOCOL.md">Connector Protocol</a>.
 * It defines valid transitions between instances of {@link ConnectorSyncStatus} and provides a method to validate these transitions.
 */
public class ConnectorSyncJobStateMachine {

    private static final Map<ConnectorSyncStatus, Set<ConnectorSyncStatus>> VALID_TRANSITIONS = Map.of(
        ConnectorSyncStatus.PENDING,
        EnumSet.of(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.CANCELED),
        ConnectorSyncStatus.IN_PROGRESS,
        EnumSet.of(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.COMPLETED, ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.ERROR),
        ConnectorSyncStatus.COMPLETED,
        Collections.emptySet(),
        ConnectorSyncStatus.SUSPENDED,
        EnumSet.of(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.CANCELED),
        ConnectorSyncStatus.CANCELING,
        EnumSet.of(ConnectorSyncStatus.CANCELED, ConnectorSyncStatus.ERROR),
        ConnectorSyncStatus.CANCELED,
        Collections.emptySet(),
        ConnectorSyncStatus.ERROR,
        Collections.emptySet()
    );

    /**
     * Checks if a transition from one {@link ConnectorSyncStatus} to another is valid.
     *
     * @param current The current {@link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     * @param next The proposed next {link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     */
    public static boolean isValidTransition(ConnectorSyncStatus current, ConnectorSyncStatus next) {
        return validNextStates(current).contains(next);
    }

    /**
     * Throws {@link ConnectorSyncJobInvalidStatusTransitionException} if a
     * transition from one {@link ConnectorSyncStatus} to another is invalid.
     *
     * @param current The current {@link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     * @param next The proposed next {@link ConnectorSyncStatus} of the {@link ConnectorSyncJob}.
     */
    public static void assertValidStateTransition(ConnectorSyncStatus current, ConnectorSyncStatus next)
        throws ConnectorSyncJobInvalidStatusTransitionException {
        if (isValidTransition(current, next)) return;
        throw new ConnectorSyncJobInvalidStatusTransitionException(current, next);
    }

    public static Set<ConnectorSyncStatus> validNextStates(ConnectorSyncStatus current) {
        return VALID_TRANSITIONS.getOrDefault(current, Collections.emptySet());
    }
}
