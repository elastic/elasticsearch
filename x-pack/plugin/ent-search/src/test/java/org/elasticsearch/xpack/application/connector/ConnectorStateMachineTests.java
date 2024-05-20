/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.test.ESTestCase;

public class ConnectorStateMachineTests extends ESTestCase {

    public void testValidTransitionFromCreated() {
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CREATED, ConnectorStatus.NEEDS_CONFIGURATION));
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CREATED, ConnectorStatus.ERROR));
    }

    public void testInvalidTransitionFromCreated() {
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.CREATED, ConnectorStatus.CONFIGURED));
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.CREATED, ConnectorStatus.CONNECTED));
    }

    public void testValidTransitionFromNeedsConfiguration() {
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.NEEDS_CONFIGURATION, ConnectorStatus.CONFIGURED));
    }

    public void testInvalidTransitionFromNeedsConfiguration() {
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.NEEDS_CONFIGURATION, ConnectorStatus.CREATED));
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.NEEDS_CONFIGURATION, ConnectorStatus.CONNECTED));
    }

    public void testValidTransitionFromConfigured() {
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONFIGURED, ConnectorStatus.NEEDS_CONFIGURATION));
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONFIGURED, ConnectorStatus.CONNECTED));
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONFIGURED, ConnectorStatus.ERROR));
    }

    public void testInvalidTransitionFromConfigured() {
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONFIGURED, ConnectorStatus.CREATED));
    }

    public void testValidTransitionFromConnected() {
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONNECTED, ConnectorStatus.CONFIGURED));
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONNECTED, ConnectorStatus.ERROR));
    }

    public void testInvalidTransitionFromConnected() {
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONNECTED, ConnectorStatus.CREATED));
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.CONNECTED, ConnectorStatus.NEEDS_CONFIGURATION));
    }

    public void testValidTransitionFromError() {
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.ERROR, ConnectorStatus.CONNECTED));
        assertTrue(ConnectorStateMachine.isValidTransition(ConnectorStatus.ERROR, ConnectorStatus.CONFIGURED));
    }

    public void testInvalidTransitionFromError() {
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.ERROR, ConnectorStatus.CREATED));
        assertFalse(ConnectorStateMachine.isValidTransition(ConnectorStatus.ERROR, ConnectorStatus.NEEDS_CONFIGURATION));
    }

    public void testTransitionToSameState() {
        for (ConnectorStatus state : ConnectorStatus.values()) {
            assertFalse("Transition from " + state + " to itself should be invalid", ConnectorStateMachine.isValidTransition(state, state));
        }
    }

    public void testAssertValidStateTransition_ExpectExceptionOnInvalidTransition() {
        assertThrows(
            ConnectorInvalidStatusTransitionException.class,
            () -> ConnectorStateMachine.assertValidStateTransition(ConnectorStatus.CREATED, ConnectorStatus.CONFIGURED)
        );
    }

    public void testAssertValidStateTransition_ExpectNoExceptionOnValidTransition() {
        ConnectorStatus prevStatus = ConnectorStatus.CREATED;
        ConnectorStatus nextStatus = ConnectorStatus.ERROR;

        try {
            ConnectorStateMachine.assertValidStateTransition(prevStatus, nextStatus);
        } catch (ConnectorInvalidStatusTransitionException e) {
            fail(
                "Did not expect "
                    + ConnectorInvalidStatusTransitionException.class.getSimpleName()
                    + " to be thrown for valid state transition ["
                    + prevStatus
                    + "] -> "
                    + "["
                    + nextStatus
                    + "]."
            );
        }
    }
}
