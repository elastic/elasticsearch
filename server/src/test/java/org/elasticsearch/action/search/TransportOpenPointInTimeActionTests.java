/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class TransportOpenPointInTimeActionTests extends ESTestCase {

    public void testAnUnreachableProjectFailsAStrictOpen() {
        IllegalStateException down = new IllegalStateException("link down");
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, Map.of("P1", down));
        assertThat(failure, instanceOf(RemoteTransportException.class));
        assertSame(down, failure.getCause());
        assertThat(failure.getMessage(), containsString("[P1]"));
        assertEquals(0, down.getSuppressed().length);
    }

    public void testPartialResultsLeaveAnUnreachableProjectToTheOpen() {
        assertNull(
            TransportOpenPointInTimeAction.failureForUnreachableProjects(true, Map.of("P1", new IllegalStateException("link down")))
        );
    }

    public void testACompleteResolutionDoesNotFail() {
        assertNull(TransportOpenPointInTimeAction.failureForUnreachableProjects(randomBoolean(), Map.of()));
    }

    public void testLaterProjectsAreSuppressedOnTheFirstFailure() {
        IllegalStateException first = new IllegalStateException("a");
        IllegalStateException second = new IllegalStateException("b");
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(
            false,
            Map.of("a-cluster", first, "b-cluster", second)
        );
        assertThat(failure, instanceOf(RemoteTransportException.class));
        assertSame(first, failure.getCause());
        assertThat(failure.getMessage(), containsString("[a-cluster]"));
        assertThat(failure.getSuppressed(), arrayContaining(instanceOf(RemoteTransportException.class)));
        assertSame(second, failure.getSuppressed()[0].getCause());
        assertThat(failure.getSuppressed()[0].getMessage(), containsString("[b-cluster]"));
        assertEquals(0, first.getSuppressed().length);
        assertEquals(0, second.getSuppressed().length);
    }
}
