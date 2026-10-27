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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;

/**
 * Pins the channel {@link AsyncExternalSourceOperator} hands the producer's warnings to.
 *
 * <p>They have to land in the driver's {@link DriverContext} sink, which {@code DriverCompletionInfo} ships back from
 * whatever node and thread ran the scan for the coordinator to re-emit. The earlier route — {@code HeaderWarning} on
 * the driver thread — reached the client only when the query happened to complete on a thread that inherited that
 * header: a quick local scan whose {@code ComputeListener} closed on the request thread lost it
 * (elastic/elasticsearch#160158), as did a read the planner shipped to a data node (elastic/esql-planning#1837).
 */
public class AsyncExternalSourceOperatorWarningsTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    /**
     * Both warning kinds the buffer accumulates reach the sink: the informational ones a format reader relays through
     * {@code FormatReadContext#informationalWarningSink} and the partial-results one the streaming truncation path
     * records. Neither is emitted by the producer itself, so {@code close()} is the only hand-off point.
     */
    public void testCloseDepositsBufferedWarningsIntoTheDriverSink() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024);
        buffer.recordInformationalWarning("declared column [department] is not present in some source files and reads null there");
        buffer.recordWarning("a record exceeded max_record_size and was truncated");

        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null);
        new AsyncExternalSourceOperator(buffer, driverContext, ExternalSourceMetrics.NOOP, "s3").close();

        driverContext.finish();
        assertThat(
            driverContext.warnings(),
            containsInAnyOrder(
                "declared column [department] is not present in some source files and reads null there",
                "a record exceeded max_record_size and was truncated"
            )
        );
        assertThat("the buffer is drained, so a second close cannot double-report", buffer.pollWarning(), nullValue());
    }

    /** A read that recorded nothing must leave the sink empty rather than depositing an empty or null entry. */
    public void testCloseWithoutWarningsLeavesTheDriverSinkEmpty() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null);
        new AsyncExternalSourceOperator(new AsyncExternalSourceBuffer(1024), driverContext, ExternalSourceMetrics.NOOP, "s3").close();

        driverContext.finish();
        assertThat(driverContext.warnings(), empty());
    }
}
