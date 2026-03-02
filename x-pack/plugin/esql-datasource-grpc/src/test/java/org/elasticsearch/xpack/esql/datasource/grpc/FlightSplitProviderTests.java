/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FlightSplitProviderTests extends ESTestCase {

    public void testSingleEndpointProducesOneSplit() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            FlightSplitProvider provider = new FlightSplitProvider();
            String endpoint = "flight://localhost:" + server.port();
            Map<String, Object> config = Map.of("endpoint", endpoint, "target", "employees");
            SplitDiscoveryContext context = new SplitDiscoveryContext(null, FileSet.UNRESOLVED, config, null, null);

            List<ExternalSplit> splits = provider.discoverSplits(context);
            assertEquals(1, splits.size());

            FlightSplit split = (FlightSplit) splits.get(0);
            assertEquals("flight", split.sourceType());
            assertNotNull(split.ticketBytes());
            assertTrue(split.ticketBytes().length > 0);
            assertEquals(100, split.estimatedRows());
        }
    }

    public void testMultiEndpointProducesMultipleSplits() throws IOException {
        int numEndpoints = 4;
        try (EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints)) {
            FlightSplitProvider provider = new FlightSplitProvider();
            String endpoint = "flight://localhost:" + server.port();
            Map<String, Object> config = Map.of("endpoint", endpoint, "target", "employees");
            SplitDiscoveryContext context = new SplitDiscoveryContext(null, FileSet.UNRESOLVED, config, null, null);

            List<ExternalSplit> splits = provider.discoverSplits(context);
            assertEquals(numEndpoints, splits.size());

            long totalEstimated = 0;
            for (ExternalSplit s : splits) {
                FlightSplit fs = (FlightSplit) s;
                assertEquals("flight", fs.sourceType());
                assertNotNull(fs.ticketBytes());
                assertTrue(fs.ticketBytes().length > 0);
                assertNotNull(fs.location());
                assertTrue(fs.estimatedRows() > 0);
                totalEstimated += fs.estimatedRows();
            }
            assertEquals(100, totalEstimated);
        }
    }

    public void testMissingEndpointReturnsEmpty() {
        FlightSplitProvider provider = new FlightSplitProvider();
        Map<String, Object> config = Map.of("target", "employees");
        SplitDiscoveryContext context = new SplitDiscoveryContext(null, FileSet.UNRESOLVED, config, null, null);

        List<ExternalSplit> splits = provider.discoverSplits(context);
        assertEquals(0, splits.size());
    }

    public void testMissingTargetReturnsEmpty() {
        FlightSplitProvider provider = new FlightSplitProvider();
        Map<String, Object> config = Map.of("endpoint", "flight://localhost:12345");
        SplitDiscoveryContext context = new SplitDiscoveryContext(null, FileSet.UNRESOLVED, config, null, null);

        List<ExternalSplit> splits = provider.discoverSplits(context);
        assertEquals(0, splits.size());
    }
}
