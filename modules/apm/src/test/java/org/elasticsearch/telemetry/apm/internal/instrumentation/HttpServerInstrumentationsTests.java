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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class HttpServerInstrumentationsTests extends ESTestCase {

    final HttpServerInstrumentation first = mock(HttpServerInstrumentation.class);
    final HttpServerInstrumentation second = mock(HttpServerInstrumentation.class);
    final HttpServerInstrumentations instrumentations = new HttpServerInstrumentations(List.of(first, second));

    final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    final RestRequest request = new FakeRestRequest();
    final RestResponse response = new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);

    public void testStartDelegatesToAllInOrder() {
        instrumentations.start(threadContext, request, "route");

        var inOrder = inOrder(first, second);
        inOrder.verify(first).start(threadContext, request, "route");
        inOrder.verify(second).start(threadContext, request, "route");
        inOrder.verifyNoMoreInteractions();
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
        when(first.prepareEnd(threadContext, request, response)).thenReturn(firstReleasable);
        when(second.prepareEnd(threadContext, request, response)).thenReturn(secondReleasable);

        Releasable aggregate = instrumentations.prepareEnd(threadContext, request, response);

        verifyNoInteractions(firstReleasable, secondReleasable);

        aggregate.close();

        var inOrder = inOrder(firstReleasable, secondReleasable);
        inOrder.verify(secondReleasable).close();
        inOrder.verify(firstReleasable).close();
        inOrder.verifyNoMoreInteractions();
    }
}
