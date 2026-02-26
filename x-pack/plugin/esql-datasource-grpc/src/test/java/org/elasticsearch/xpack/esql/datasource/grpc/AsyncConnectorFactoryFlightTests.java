/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncConnectorFactoryFlightTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testFullPipelineProducesCorrectPages() throws Exception {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            String endpoint = "flight://localhost:" + server.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);
                ResultCursor cursor = connector.execute(request, Split.SINGLE);

                List<Page> pages = new ArrayList<>();
                try {
                    while (cursor.hasNext()) {
                        pages.add(cursor.next());
                    }
                } finally {
                    cursor.close();
                }

                int totalRows = 0;
                for (Page page : pages) {
                    totalRows += page.getPositionCount();
                }
                assertEquals(100, totalRows);

                // Verify first page has correct data
                Page firstPage = pages.get(0);
                assertTrue(firstPage.getPositionCount() > 0);
                assertEquals(6, firstPage.getBlockCount());

                // emp_no of first row should be 10001
                IntBlock empNoBlock = firstPage.getBlock(0);
                assertEquals(10001, empNoBlock.getInt(0));

                // first_name of first row should be "Georgi"
                BytesRefBlock firstNameBlock = firstPage.getBlock(1);
                assertEquals(new BytesRef("Georgi"), firstNameBlock.getBytesRef(0, new BytesRef()));

                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
    }

    public void testConnectorWithDrainUtils() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            String endpoint = "flight://localhost:" + server.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);
                AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

                ResultCursor cursor = connector.execute(request, Split.SINGLE);
                executor.execute(() -> {
                    try {
                        org.elasticsearch.xpack.esql.datasources.ExternalSourceDrainUtils.drainPages(cursor, buffer);
                        buffer.finish(false);
                    } catch (Exception e) {
                        buffer.onFailure(e);
                    } finally {
                        try {
                            cursor.close();
                        } catch (IOException ignored) {}
                    }
                });

                int totalRows = 0;
                while (buffer.noMoreInputs() == false || buffer.size() > 0) {
                    Page page = buffer.pollPage();
                    if (page != null) {
                        totalRows += page.getPositionCount();
                        page.releaseBlocks();
                    } else if (buffer.noMoreInputs() == false) {
                        Thread.sleep(10);
                    }
                }
                // Drain remaining
                Page page;
                while ((page = buffer.pollPage()) != null) {
                    totalRows += page.getPositionCount();
                    page.releaseBlocks();
                }

                assertEquals(100, totalRows);
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
