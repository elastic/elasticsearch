/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceBuffer;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceDrainUtils;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncConnectorFactoryFlightTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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

                Page firstPage = pages.get(0);
                assertTrue(firstPage.getPositionCount() > 0);
                assertEquals(6, firstPage.getBlockCount());

                IntBlock empNoBlock = firstPage.getBlock(0);
                assertEquals(10001, empNoBlock.getInt(0));

                BytesRefBlock firstNameBlock = firstPage.getBlock(1);
                assertEquals(new BytesRef("Georgi"), firstNameBlock.getBytesRef(0, new BytesRef()));

                for (Page page : pages) {
                    page.releaseBlocks();
                }
            }
        }
    }

    public void testConnectorWithDrainUtils() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "drain"));
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
                AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);

                ResultCursor cursor = connector.execute(request, Split.SINGLE);
                CountDownLatch drainDone = new CountDownLatch(1);
                AtomicReference<Exception> drainError = new AtomicReference<>();
                executor.execute(
                    () -> ExternalSourceDrainUtils.drainPagesAsync(
                        cursor,
                        buffer,
                        executor,
                        ActionListener.runAfter(ActionListener.wrap(v -> {
                            buffer.finish(false);
                            drainDone.countDown();
                        }, e -> {
                            drainError.set(e);
                            buffer.onFailure(e);
                            drainDone.countDown();
                        }), () -> {
                            try {
                                cursor.close();
                            } catch (Exception ignored) {}
                        })
                    )
                );

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
                Page page;
                while ((page = buffer.pollPage()) != null) {
                    totalRows += page.getPositionCount();
                    page.releaseBlocks();
                }

                assertTrue(drainDone.await(30, TimeUnit.SECONDS));
                assertNull("Drain should not fail", drainError.get());
                assertEquals(100, totalRows);
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testMultiSplitParallelExecution() throws Exception {
        int numEndpoints = 4;
        try (EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints)) {
            String endpoint = "flight://localhost:" + server.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            List<ExternalSplit> splits = new ArrayList<>(numEndpoints);
            String loc = "grpc://localhost:" + server.port();
            for (int i = 0; i < numEndpoints; i++) {
                byte[] ticket = ("employees-part-" + i).getBytes(StandardCharsets.UTF_8);
                splits.add(new FlightSplit(ticket, loc, 100 / numEndpoints));
            }

            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);

                int totalRows = 0;
                TreeSet<Integer> allEmpNos = new TreeSet<>();
                for (ExternalSplit split : splits) {
                    try (ResultCursor cursor = connector.execute(request, split)) {
                        while (cursor.hasNext()) {
                            Page page = cursor.next();
                            IntBlock empNoBlock = page.getBlock(0);
                            for (int i = 0; i < page.getPositionCount(); i++) {
                                allEmpNos.add(empNoBlock.getInt(i));
                            }
                            totalRows += page.getPositionCount();
                            page.releaseBlocks();
                        }
                    }
                }
                assertEquals(100, totalRows);
                assertEquals(100, allEmpNos.size());
            }
        }
    }

    public void testMultiSplitWithSliceQueue() throws Exception {
        int numEndpoints = 4;
        ExecutorService executor = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "drain"));
        try (EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints)) {
            String endpoint = "flight://localhost:" + server.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            List<ExternalSplit> splits = new ArrayList<>(numEndpoints);
            String loc = "grpc://localhost:" + server.port();
            for (int i = 0; i < numEndpoints; i++) {
                byte[] ticket = ("employees-part-" + i).getBytes(StandardCharsets.UTF_8);
                splits.add(new FlightSplit(ticket, loc, 100 / numEndpoints));
            }

            ExternalSliceQueue sliceQueue = new ExternalSliceQueue(splits);
            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);
                AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
                CountDownLatch drainDone = new CountDownLatch(1);
                AtomicReference<Exception> drainError = new AtomicReference<>();

                executor.execute(() -> drainNextSplit(sliceQueue, connector, request, buffer, executor, drainDone, drainError));

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
                Page page;
                while ((page = buffer.pollPage()) != null) {
                    totalRows += page.getPositionCount();
                    page.releaseBlocks();
                }

                assertTrue(drainDone.await(30, TimeUnit.SECONDS));
                assertNull("Drain should not fail", drainError.get());
                assertEquals(100, totalRows);
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void drainNextSplit(
        ExternalSliceQueue sliceQueue,
        Connector connector,
        QueryRequest request,
        AsyncExternalSourceBuffer buffer,
        ExecutorService executor,
        CountDownLatch drainDone,
        AtomicReference<Exception> drainError
    ) {
        if (buffer.noMoreInputs()) {
            buffer.finish(false);
            drainDone.countDown();
            return;
        }
        ExternalSplit split = sliceQueue.nextSplit();
        if (split == null) {
            buffer.finish(false);
            drainDone.countDown();
            return;
        }
        ResultCursor cursor;
        try {
            cursor = connector.execute(request, split);
        } catch (Exception e) {
            buffer.onFailure(e);
            drainDone.countDown();
            return;
        }
        ExternalSourceDrainUtils.drainPagesAsync(cursor, buffer, executor, ActionListener.runAfter(ActionListener.wrap(v -> {
            try {
                executor.execute(() -> drainNextSplit(sliceQueue, connector, request, buffer, executor, drainDone, drainError));
            } catch (Exception e) {
                drainError.set(e);
                buffer.onFailure(e);
                drainDone.countDown();
            }
        }, e -> {
            drainError.set(e);
            buffer.onFailure(e);
            drainDone.countDown();
        }), () -> {
            try {
                cursor.close();
            } catch (Exception ignored) {}
        }));
    }

    public void testMultiSplitWithNullLocationUsesDefaultClient() throws Exception {
        int numEndpoints = 3;
        try (EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints)) {
            String endpoint = "flight://localhost:" + server.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            List<ExternalSplit> splits = new ArrayList<>(numEndpoints);
            for (int i = 0; i < numEndpoints; i++) {
                byte[] ticket = ("employees-part-" + i).getBytes(StandardCharsets.UTF_8);
                splits.add(new FlightSplit(ticket, null, 100 / numEndpoints));
            }

            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);

                int totalRows = 0;
                for (ExternalSplit split : splits) {
                    try (ResultCursor cursor = connector.execute(request, split)) {
                        while (cursor.hasNext()) {
                            Page page = cursor.next();
                            totalRows += page.getPositionCount();
                            page.releaseBlocks();
                        }
                    }
                }
                assertEquals(100, totalRows);
            }
        }
    }

    public void testMultiSplitAcrossDifferentServers() throws Exception {
        try (EmployeeFlightServer server1 = new EmployeeFlightServer(0, 2); EmployeeFlightServer server2 = new EmployeeFlightServer(0, 2)) {
            String endpoint = "flight://localhost:" + server1.port();
            FlightConnectorFactory factory = new FlightConnectorFactory();

            List<Attribute> attributes = FlightTypeMapping.toAttributes(EmployeeFlightServer.SCHEMA);
            List<String> projectedColumns = new ArrayList<>();
            for (Attribute attr : attributes) {
                projectedColumns.add(attr.name());
            }

            List<ExternalSplit> splits = new ArrayList<>(4);
            splits.add(new FlightSplit("employees-part-0".getBytes(StandardCharsets.UTF_8), "grpc://localhost:" + server1.port(), 25));
            splits.add(new FlightSplit("employees-part-1".getBytes(StandardCharsets.UTF_8), "grpc://localhost:" + server1.port(), 25));
            splits.add(new FlightSplit("employees-part-0".getBytes(StandardCharsets.UTF_8), "grpc://localhost:" + server2.port(), 25));
            splits.add(new FlightSplit("employees-part-1".getBytes(StandardCharsets.UTF_8), "grpc://localhost:" + server2.port(), 25));

            try (Connector connector = factory.open(Map.of("endpoint", endpoint))) {
                QueryRequest request = new QueryRequest("employees", projectedColumns, attributes, Map.of(), 1000, blockFactory);

                int totalRows = 0;
                for (ExternalSplit split : splits) {
                    try (ResultCursor cursor = connector.execute(request, split)) {
                        while (cursor.hasNext()) {
                            Page page = cursor.next();
                            totalRows += page.getPositionCount();
                            page.releaseBlocks();
                        }
                    }
                }
                assertEquals(200, totalRows);
            }
        }
    }
}
