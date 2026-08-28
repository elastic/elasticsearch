/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;

public class TransportSerializationExceptionTests extends ESTestCase {

    public void testCircuitBreakingCauseReportsBreakerStatus() {
        var cbe = new CircuitBreakingException("[parent] Data too large", randomFrom(CircuitBreaker.Durability.values()));
        Throwable cause = randomBoolean() ? cbe : new RuntimeException("wrapped", cbe);
        var exception = new TransportSerializationException("Failed to deserialize response from handler [h]", cause);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, exception.status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(exception));
        // the exception-response path wraps the serialization exception in a RemoteTransportException
        assertEquals(
            RestStatus.TOO_MANY_REQUESTS,
            ExceptionsHelper.status(new RemoteTransportException(exception.getMessage(), exception))
        );
    }

    public void testGenericCauseReportsInternalServerError() {
        var exception = new TransportSerializationException(
            "Failed to deserialize response from handler [h]",
            randomBoolean() ? new EOFException("unexpected end of stream") : new IllegalArgumentException("bad vint")
        );
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, exception.status());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(exception));
    }
}
