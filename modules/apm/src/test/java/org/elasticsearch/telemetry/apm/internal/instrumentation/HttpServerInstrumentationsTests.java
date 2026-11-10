/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class HttpServerInstrumentationsTests extends ESTestCase {

    final HttpServerInstrumentation first = mock(HttpServerInstrumentation.class);
    final HttpServerInstrumentation second = mock(HttpServerInstrumentation.class);
    final HttpServerInstrumentations instrumentations = new HttpServerInstrumentations(List.of(first, second));

    final RestRequest request = new FakeRestRequest();
    final RestResponse response = new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);

    public void testStartDelegatesToAllInOrder() {
        instrumentations.start(request, "route");

        var inOrder = inOrder(first, second);
        inOrder.verify(first).start(request, "route");
        inOrder.verify(second).start(request, "route");
        inOrder.verifyNoMoreInteractions();
    }

    /**
     * Each instrumentation restores what it found, which includes what the ones before it set up, so the contexts are closed backwards.
     */
    public void testStartReturnsAContextThatRestoresTheContextsInReverseOrder() {
        final List<String> closed = new ArrayList<>();
        when(first.start(request, "route")).thenReturn(() -> closed.add("first"));
        when(second.start(request, "route")).thenReturn(() -> closed.add("second"));

        try (var ignored = instrumentations.start(request, "route")) {
            assertThat(closed, empty());
        }

        assertThat(closed, contains("second", "first"));
    }

    public void testRecordExceptionDelegatesToAllInOrder() {
        var exception = new RuntimeException("test");

        instrumentations.recordException(request, exception);

        var inOrder = inOrder(first, second);
        inOrder.verify(first).recordException(request, exception);
        inOrder.verify(second).recordException(request, exception);
        inOrder.verifyNoMoreInteractions();
    }

    public void testPrepareEndDoesNotCloseDelegatesUntilAggregateIsClosed() {
        Releasable firstReleasable = mock(Releasable.class);
        Releasable secondReleasable = mock(Releasable.class);
        when(first.prepareEnd(request, response)).thenReturn(firstReleasable);
        when(second.prepareEnd(request, response)).thenReturn(secondReleasable);

        Releasable aggregate = instrumentations.prepareEnd(request, response);

        verifyNoInteractions(firstReleasable, secondReleasable);

        aggregate.close();

        var inOrder = inOrder(firstReleasable, secondReleasable);
        inOrder.verify(secondReleasable).close();
        inOrder.verify(firstReleasable).close();
        inOrder.verifyNoMoreInteractions();
    }
}
