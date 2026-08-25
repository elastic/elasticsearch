/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Pins where the source operator delivers the warnings its background reader recorded.
 *
 * <p>They must land in the driver context's structured sink, which {@code DriverCompletionInfo} merges and
 * {@code TransportEsqlQueryAction} replays onto the response. Emitting them into the thread context via
 * {@code HeaderWarning} instead only reached the client by luck — {@code ExchangeService} strips raw
 * {@code Warning} headers off the thread context on every page fetch — which is how external-source
 * warnings went intermittently missing (#153187, #153158).
 */
public class AsyncExternalSourceOperatorWarningsTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public void testCloseDeliversRecordedWarningsToTheDriverContext() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null);
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);
        // Both relay methods the readers use: per-record null-fill/skip relays and the truncation warning.
        buffer.recordInformationalWarning("values could not be coerced to the declared column type");
        buffer.recordWarning("the read was truncated");

        try (AsyncExternalSourceOperator operator = new AsyncExternalSourceOperator(buffer, driverContext::addWarning)) {
            // close() is what drains the buffer, so nothing has been delivered yet.
            assertTrue(buffer.noMoreInputs() == false);
        }

        driverContext.finish();
        assertThat(
            driverContext.warnings(),
            containsInAnyOrder("values could not be coerced to the declared column type", "the read was truncated")
        );
    }

    public void testCloseWithNoRecordedWarningsAddsNothing() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null);
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        new AsyncExternalSourceOperator(buffer, driverContext::addWarning).close();

        driverContext.finish();
        assertTrue(driverContext.warnings().isEmpty());
    }
}
