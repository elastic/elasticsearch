/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class EmployeeFlightServerTests extends ESTestCase {

    public void testServerStartsAndReportsRows() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            assertTrue(server.port() > 0);
            assertEquals(100, server.totalRows());
        }
    }

    public void testGetSchemaReturnsCorrectFields() throws Exception {
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            SchemaResult schemaResult = client.getSchema(FlightDescriptor.path("employees"));
            Schema schema = schemaResult.getSchema();

            assertEquals(6, schema.getFields().size());
            assertEquals("emp_no", schema.getFields().get(0).getName());
            assertEquals("first_name", schema.getFields().get(1).getName());
            assertEquals("last_name", schema.getFields().get(2).getName());
            assertEquals("salary", schema.getFields().get(3).getName());
            assertEquals("still_hired", schema.getFields().get(4).getName());
            assertEquals("height", schema.getFields().get(5).getName());
        }
    }

    public void testGetFlightInfoReturnsSingleEndpoint() throws Exception {
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            assertEquals(1, info.getEndpoints().size());
            assertEquals(100, info.getRecords());
        }
    }

    public void testGetStreamReturnsAllRows() throws Exception {
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            Ticket ticket = info.getEndpoints().get(0).getTicket();

            int totalRows = 0;
            try (FlightStream stream = client.getStream(ticket)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    totalRows += root.getRowCount();
                }
            }
            assertEquals(100, totalRows);
        }
    }

    public void testFirstEmployeeData() throws Exception {
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            Ticket ticket = info.getEndpoints().get(0).getTicket();

            try (FlightStream stream = client.getStream(ticket)) {
                assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();

                IntVector empNoVec = (IntVector) root.getVector("emp_no");
                VarCharVector firstNameVec = (VarCharVector) root.getVector("first_name");
                VarCharVector lastNameVec = (VarCharVector) root.getVector("last_name");

                assertEquals(10001, empNoVec.get(0));
                assertEquals("Georgi", new String(firstNameVec.get(0), java.nio.charset.StandardCharsets.UTF_8).trim());
                assertEquals("Facello", new String(lastNameVec.get(0), java.nio.charset.StandardCharsets.UTF_8).trim());
            }
        }
    }

    public void testMultiEndpointReturnsCorrectEndpointCount() throws Exception {
        int numEndpoints = 4;
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            assertEquals(numEndpoints, info.getEndpoints().size());
            assertEquals(100, info.getRecords());
        }
    }

    public void testMultiEndpointPartitionsAreDisjointAndComplete() throws Exception {
        int numEndpoints = 4;
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            List<FlightEndpoint> endpoints = info.getEndpoints();
            assertEquals(numEndpoints, endpoints.size());

            int totalRows = 0;
            java.util.Set<Integer> allEmpNos = new java.util.TreeSet<>();
            for (FlightEndpoint ep : endpoints) {
                try (FlightStream stream = client.getStream(ep.getTicket())) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        IntVector empNoVec = (IntVector) root.getVector("emp_no");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            assertTrue("Duplicate emp_no: " + empNoVec.get(i), allEmpNos.add(empNoVec.get(i)));
                        }
                        totalRows += root.getRowCount();
                    }
                }
            }
            assertEquals(100, totalRows);
            assertEquals(100, allEmpNos.size());
        }
    }

    public void testMultiEndpointEachPartitionHasRows() throws Exception {
        int numEndpoints = 5;
        try (
            EmployeeFlightServer server = new EmployeeFlightServer(0, numEndpoints);
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.port())).build()
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path("employees"));
            for (FlightEndpoint ep : info.getEndpoints()) {
                int partRows = 0;
                try (FlightStream stream = client.getStream(ep.getTicket())) {
                    while (stream.next()) {
                        partRows += stream.getRoot().getRowCount();
                    }
                }
                assertTrue("Each partition must have rows, got: " + partRows, partRows > 0);
            }
        }
    }
}
