/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * In-memory Arrow Flight server that serves employee data from the employees.csv test fixture.
 * Uses a subset of columns: emp_no, first_name, last_name, salary, still_hired, height.
 *
 * Supports two modes:
 * <ul>
 *   <li>Single-endpoint: all rows via one ticket ({@code new EmployeeFlightServer(0)})</li>
 *   <li>Multi-endpoint: rows partitioned across N endpoints, each with its own ticket
 *       ({@code new EmployeeFlightServer(0, numEndpoints)})</li>
 * </ul>
 */
public class EmployeeFlightServer implements Closeable {

    public static final Schema SCHEMA = new Schema(
        List.of(
            new Field("emp_no", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("first_name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("last_name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("salary", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("still_hired", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("height", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
        )
    );

    private final BufferAllocator allocator;
    private final FlightServer server;
    private final Location location;
    private final int numEndpoints;
    private int[] allEmpNos;
    private String[] allFirstNames;
    private String[] allLastNames;
    private int[] allSalaries;
    private boolean[] allStillHired;
    private double[] allHeights;
    private int totalRows;

    public EmployeeFlightServer(int port) throws IOException {
        this(port, 1);
    }

    public EmployeeFlightServer(int port, int numEndpoints) throws IOException {
        if (numEndpoints < 1) {
            throw new IllegalArgumentException("numEndpoints must be >= 1, got: " + numEndpoints);
        }
        this.allocator = new RootAllocator();
        this.location = Location.forGrpcInsecure("localhost", port);
        this.numEndpoints = numEndpoints;
        loadEmployees();
        this.server = FlightServer.builder(allocator, location, new EmployeeProducer()).build();
        this.server.start();
    }

    public int port() {
        return server.getPort();
    }

    public int totalRows() {
        return totalRows;
    }

    public int numEndpoints() {
        return numEndpoints;
    }

    private void loadEmployees() throws IOException {
        try (
            InputStream is = getClass().getResourceAsStream("/data/employees.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            String header = reader.readLine();
            String[] columns = header.split(",");

            int empNoIdx = -1, firstNameIdx = -1, lastNameIdx = -1, salaryIdx = -1, stillHiredIdx = -1, heightIdx = -1;
            for (int i = 0; i < columns.length; i++) {
                String colName = columns[i].trim().split(":")[0];
                switch (colName) {
                    case "emp_no" -> empNoIdx = i;
                    case "first_name" -> firstNameIdx = i;
                    case "last_name" -> lastNameIdx = i;
                    case "salary" -> salaryIdx = i;
                    case "still_hired" -> stillHiredIdx = i;
                    case "height" -> heightIdx = i;
                }
            }

            List<String[]> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                rows.add(line.split(","));
            }
            totalRows = rows.size();

            allEmpNos = new int[totalRows];
            allFirstNames = new String[totalRows];
            allLastNames = new String[totalRows];
            allSalaries = new int[totalRows];
            allStillHired = new boolean[totalRows];
            allHeights = new double[totalRows];

            for (int i = 0; i < totalRows; i++) {
                String[] fields = rows.get(i);
                allEmpNos[i] = Integer.parseInt(fields[empNoIdx].trim());
                allFirstNames[i] = fields[firstNameIdx].trim();
                allLastNames[i] = fields[lastNameIdx].trim();
                allSalaries[i] = Integer.parseInt(fields[salaryIdx].trim());
                allStillHired[i] = org.elasticsearch.core.Booleans.parseBoolean(fields[stillHiredIdx].trim());
                allHeights[i] = Double.parseDouble(fields[heightIdx].trim());
            }
        }
    }

    private int partitionStart(int partitionIndex) {
        int partSize = totalRows / numEndpoints;
        return partitionIndex * partSize;
    }

    private int partitionEnd(int partitionIndex) {
        int partSize = totalRows / numEndpoints;
        if (partitionIndex == numEndpoints - 1) {
            return totalRows;
        }
        return (partitionIndex + 1) * partSize;
    }

    private class EmployeeProducer extends NoOpFlightProducer {

        @Override
        public SchemaResult getSchema(FlightProducer.CallContext context, FlightDescriptor descriptor) {
            return new SchemaResult(SCHEMA);
        }

        @Override
        public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
            if (numEndpoints == 1) {
                FlightEndpoint endpoint = new FlightEndpoint(new Ticket("employees".getBytes(StandardCharsets.UTF_8)), location);
                return new FlightInfo(SCHEMA, descriptor, Collections.singletonList(endpoint), -1, totalRows);
            }
            List<FlightEndpoint> endpoints = new ArrayList<>(numEndpoints);
            for (int i = 0; i < numEndpoints; i++) {
                String ticketId = "employees-part-" + i;
                endpoints.add(new FlightEndpoint(new Ticket(ticketId.getBytes(StandardCharsets.UTF_8)), location));
            }
            return new FlightInfo(SCHEMA, descriptor, endpoints, -1, totalRows);
        }

        @Override
        public void getStream(FlightProducer.CallContext context, Ticket ticket, FlightProducer.ServerStreamListener listener) {
            String ticketStr = new String(ticket.getBytes(), StandardCharsets.UTF_8);

            int startRow;
            int endRow;
            if ("employees".equals(ticketStr)) {
                startRow = 0;
                endRow = totalRows;
            } else if (ticketStr.startsWith("employees-part-")) {
                int partIndex;
                try {
                    partIndex = Integer.parseInt(ticketStr.substring("employees-part-".length()));
                } catch (NumberFormatException e) {
                    listener.error(CallStatus.NOT_FOUND.withDescription("Invalid partition ticket: " + ticketStr).toRuntimeException());
                    return;
                }
                if (partIndex < 0 || partIndex >= numEndpoints) {
                    listener.error(CallStatus.NOT_FOUND.withDescription("Partition index out of range: " + partIndex).toRuntimeException());
                    return;
                }
                startRow = partitionStart(partIndex);
                endRow = partitionEnd(partIndex);
            } else {
                listener.error(CallStatus.NOT_FOUND.withDescription("Unknown ticket: " + ticketStr).toRuntimeException());
                return;
            }

            int rowCount = endRow - startRow;
            try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                listener.start(root);

                IntVector empNoVec = (IntVector) root.getVector("emp_no");
                VarCharVector firstNameVec = (VarCharVector) root.getVector("first_name");
                VarCharVector lastNameVec = (VarCharVector) root.getVector("last_name");
                IntVector salaryVec = (IntVector) root.getVector("salary");
                BitVector stillHiredVec = (BitVector) root.getVector("still_hired");
                Float8Vector heightVec = (Float8Vector) root.getVector("height");

                root.allocateNew();
                for (int i = 0; i < rowCount; i++) {
                    int srcIdx = startRow + i;
                    empNoVec.setSafe(i, allEmpNos[srcIdx]);
                    firstNameVec.setSafe(i, allFirstNames[srcIdx].getBytes(StandardCharsets.UTF_8));
                    lastNameVec.setSafe(i, allLastNames[srcIdx].getBytes(StandardCharsets.UTF_8));
                    salaryVec.setSafe(i, allSalaries[srcIdx]);
                    stillHiredVec.setSafe(i, allStillHired[srcIdx] ? 1 : 0);
                    heightVec.setSafe(i, allHeights[srcIdx]);
                }
                root.setRowCount(rowCount);
                listener.putNext();
                listener.completed();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            server.shutdown();
            server.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing FlightServer", e);
        } finally {
            allocator.close();
        }
    }
}
