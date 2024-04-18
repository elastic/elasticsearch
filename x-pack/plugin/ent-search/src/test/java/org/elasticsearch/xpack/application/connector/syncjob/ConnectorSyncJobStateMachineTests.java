/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;

public class ConnectorSyncJobStateMachineTests extends ESTestCase {

    public void testValidTransitionFromPending() {
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.IN_PROGRESS));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.CANCELED));
    }

    public void testInvalidTransitionFromPending() {
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.COMPLETED));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.SUSPENDED));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.CANCELING));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.ERROR));
    }

    public void testValidTransitionFromInProgress() {
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.CANCELING));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.COMPLETED));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.SUSPENDED));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.ERROR));
    }

    public void testInvalidTransitionFromInProgress() {
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.PENDING));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.IN_PROGRESS, ConnectorSyncStatus.CANCELED));
    }

    public void testNoValidTransitionsFromCompleted() {
        for (ConnectorSyncStatus state : ConnectorSyncStatus.values()) {
            assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.COMPLETED, state));
        }
    }

    public void testValidTransitionFromSuspended() {
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.IN_PROGRESS));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.CANCELED));
    }

    public void testInvalidTransitionFromSuspended() {
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.PENDING));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.COMPLETED));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.CANCELING));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.SUSPENDED, ConnectorSyncStatus.ERROR));
    }

    public void testValidTransitionFromCanceling() {
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.CANCELED));
        assertTrue(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.ERROR));
    }

    public void testInvalidTransitionFromCanceling() {
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.PENDING));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.IN_PROGRESS));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.COMPLETED));
        assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELING, ConnectorSyncStatus.SUSPENDED));
    }

    public void testNoValidTransitionsFromCanceled() {
        for (ConnectorSyncStatus state : ConnectorSyncStatus.values()) {
            assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.CANCELED, state));
        }
    }

    public void testNoValidTransitionsFromError() {
        for (ConnectorSyncStatus state : ConnectorSyncStatus.values()) {
            assertFalse(ConnectorSyncJobStateMachine.isValidTransition(ConnectorSyncStatus.ERROR, state));
        }
    }

    public void testTransitionToSameState() {
        for (ConnectorSyncStatus state : ConnectorSyncStatus.values()) {
            assertFalse(
                "Transition from " + state + " to itself should be invalid",
                ConnectorSyncJobStateMachine.isValidTransition(state, state)
            );
        }
    }

    public void testAssertValidStateTransition_ExpectExceptionOnInvalidTransition() {
        assertThrows(
            ConnectorSyncJobInvalidStatusTransitionException.class,
            () -> ConnectorSyncJobStateMachine.assertValidStateTransition(ConnectorSyncStatus.PENDING, ConnectorSyncStatus.CANCELING)
        );
    }

    public void testAssertValidStateTransition_ExpectNoExceptionOnValidTransition() {
        ConnectorSyncStatus prevStatus = ConnectorSyncStatus.PENDING;
        ConnectorSyncStatus nextStatus = ConnectorSyncStatus.CANCELED;

        try {
            ConnectorSyncJobStateMachine.assertValidStateTransition(prevStatus, nextStatus);
        } catch (ConnectorSyncJobInvalidStatusTransitionException e) {
            fail(
                "Did not expect "
                    + ConnectorSyncJobInvalidStatusTransitionException.class.getSimpleName()
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
