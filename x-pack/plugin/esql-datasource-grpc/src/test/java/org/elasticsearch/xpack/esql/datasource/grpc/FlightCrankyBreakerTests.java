/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A read of a Flight stream when the circuit breaker trips at a random allocation: whichever allocation fails, the query fails with the
 * breaker's exception and every byte that was charged, the Arrow buffers of the blocks and of the stream, is released again.
 */
public class FlightCrankyBreakerTests extends ESTestCase {

    public void testNothingIsLeftWhenTheBreakerTripsDuringAPlainRead() throws Exception {
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BlockFactory crankyFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking()
        ).build();
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);

        List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
        List<String> projectedColumns = new ArrayList<>();
        for (Attribute attribute : attributes) {
            projectedColumns.add(attribute.name());
        }

        int tripped = 0; // the cranky breaker trips one charged allocation in 20 and a read charges about one: 400 reads always trip
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0);
            Connector connector = new FlightConnectorFactory().open(Map.of("endpoint", "flight://localhost:" + server.port()))
        ) {
            for (int i = 0; i < 400; i++) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, crankyFactory);
                try (ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
                    while (cursor.hasNext()) {
                        cursor.next().releaseBlocks();
                    }
                } catch (RuntimeException e) {
                    CircuitBreakingException cranky = (CircuitBreakingException) ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
                    if (cranky == null) {
                        throw e;
                    }
                    assertThat(cranky.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                    tripped++;
                }
                assertThat("the breaker was not released, iteration " + i, breaker.getUsed(), equalTo(0L));
                assertThat(
                    "a stream was left open, iteration " + i,
                    crankyFactory.arrowAllocator().getChildAllocators().size(),
                    equalTo(0)
                );
                assertThat("a block was leaked, iteration " + i, crankyFactory.arrowAllocator().getAllocatedMemory(), equalTo(0L));
            }
        }
        assertThat("the breaker never tripped, so nothing was tested", tripped, greaterThan(0));
    }
}
