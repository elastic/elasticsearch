/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link ConnectorStateMachine} class manages state transitions for instances of {@link Connector}
 * in accordance with the <a href="https://github.com/elastic/connectors/blob/main/docs/CONNECTOR_PROTOCOL.md">Connector Protocol</a>.
 * It defines valid transitions between instances of {@link ConnectorStatus} and provides a method to validate these transitions.
 */
public class ConnectorStateMachine {

    private static final Map<ConnectorStatus, Set<ConnectorStatus>> VALID_TRANSITIONS = Map.of(
        ConnectorStatus.CREATED,
        EnumSet.of(ConnectorStatus.NEEDS_CONFIGURATION, ConnectorStatus.ERROR),
        ConnectorStatus.NEEDS_CONFIGURATION,
        EnumSet.of(ConnectorStatus.CONFIGURED, ConnectorStatus.ERROR),
        ConnectorStatus.CONFIGURED,
        EnumSet.of(ConnectorStatus.NEEDS_CONFIGURATION, ConnectorStatus.CONFIGURED, ConnectorStatus.CONNECTED, ConnectorStatus.ERROR),
        ConnectorStatus.CONNECTED,
        EnumSet.of(ConnectorStatus.CONNECTED, ConnectorStatus.CONFIGURED, ConnectorStatus.ERROR),
        ConnectorStatus.ERROR,
        EnumSet.of(ConnectorStatus.CONNECTED, ConnectorStatus.CONFIGURED, ConnectorStatus.ERROR)
    );

    /**
     * Checks if a transition from one {@link ConnectorStatus} to another is valid.
     *
     * @param current The current {@link ConnectorStatus} of the {@link Connector}.
     * @param next The proposed next {@link ConnectorStatus} of the {@link Connector}.
     */
    public static boolean isValidTransition(ConnectorStatus current, ConnectorStatus next) {
        return validNextStates(current).contains(next);
    }

    /**
     * Throws {@link ConnectorInvalidStatusTransitionException} if a
     * transition from one {@link ConnectorStatus} to another is invalid.
     *
     * @param current The current {@link ConnectorStatus} of the {@link Connector}.
     * @param next The proposed next {@link ConnectorStatus} of the {@link Connector}.
     */
    public static void assertValidStateTransition(ConnectorStatus current, ConnectorStatus next)
        throws ConnectorInvalidStatusTransitionException {
        if (isValidTransition(current, next)) return;
        throw new ConnectorInvalidStatusTransitionException(current, next);
    }

    public static Set<ConnectorStatus> validNextStates(ConnectorStatus current) {
        return VALID_TRANSITIONS.getOrDefault(current, Collections.emptySet());
    }
}
