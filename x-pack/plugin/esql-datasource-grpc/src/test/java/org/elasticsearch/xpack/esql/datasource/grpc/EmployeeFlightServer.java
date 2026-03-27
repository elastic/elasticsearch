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

/**
 * In-memory Arrow Flight server that serves employee data from the employees.csv test fixture.
 * Uses a subset of columns: emp_no, first_name, last_name, salary, still_hired, height.
 * Serves all rows via a single endpoint (no multi-split).
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
    private final List<int[]> empNos = new ArrayList<>();
    private final List<String> firstNames = new ArrayList<>();
    private final List<String> lastNames = new ArrayList<>();
    private final List<int[]> salaries = new ArrayList<>();
    private final List<boolean[]> stillHired = new ArrayList<>();
    private final List<double[]> heights = new ArrayList<>();
    private int totalRows;

    public EmployeeFlightServer(int port) throws IOException {
        this.allocator = new RootAllocator();
        this.location = Location.forGrpcInsecure("localhost", port);
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

            int batchSize = totalRows;
            int[] empNoArr = new int[batchSize];
            String[] firstNameArr = new String[batchSize];
            String[] lastNameArr = new String[batchSize];
            int[] salaryArr = new int[batchSize];
            boolean[] stillHiredArr = new boolean[batchSize];
            double[] heightArr = new double[batchSize];

            for (int i = 0; i < totalRows; i++) {
                String[] fields = rows.get(i);
                empNoArr[i] = Integer.parseInt(fields[empNoIdx].trim());
                firstNameArr[i] = fields[firstNameIdx].trim();
                lastNameArr[i] = fields[lastNameIdx].trim();
                salaryArr[i] = Integer.parseInt(fields[salaryIdx].trim());
                stillHiredArr[i] = org.elasticsearch.core.Booleans.parseBoolean(fields[stillHiredIdx].trim());
                heightArr[i] = Double.parseDouble(fields[heightIdx].trim());
            }

            empNos.add(empNoArr);
            firstNames.addAll(List.of(firstNameArr));
            lastNames.addAll(List.of(lastNameArr));
            salaries.add(salaryArr);
            stillHired.add(stillHiredArr);
            heights.add(heightArr);
        }
    }

    private class EmployeeProducer extends NoOpFlightProducer {

        @Override
        public SchemaResult getSchema(FlightProducer.CallContext context, FlightDescriptor descriptor) {
            return new SchemaResult(SCHEMA);
        }

        @Override
        public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
            FlightEndpoint endpoint = new FlightEndpoint(new Ticket("employees".getBytes(StandardCharsets.UTF_8)), location);
            return new FlightInfo(SCHEMA, descriptor, Collections.singletonList(endpoint), -1, totalRows);
        }

        @Override
        public void getStream(FlightProducer.CallContext context, Ticket ticket, FlightProducer.ServerStreamListener listener) {
            String ticketStr = new String(ticket.getBytes(), StandardCharsets.UTF_8);
            if ("employees".equals(ticketStr) == false) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Unknown ticket: " + ticketStr).toRuntimeException());
                return;
            }

            try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                listener.start(root);

                IntVector empNoVec = (IntVector) root.getVector("emp_no");
                VarCharVector firstNameVec = (VarCharVector) root.getVector("first_name");
                VarCharVector lastNameVec = (VarCharVector) root.getVector("last_name");
                IntVector salaryVec = (IntVector) root.getVector("salary");
                BitVector stillHiredVec = (BitVector) root.getVector("still_hired");
                Float8Vector heightVec = (Float8Vector) root.getVector("height");

                int[] empNoArr = empNos.get(0);
                int[] salaryArr = salaries.get(0);
                boolean[] stillHiredArr = EmployeeFlightServer.this.stillHired.get(0);
                double[] heightArr = EmployeeFlightServer.this.heights.get(0);

                root.allocateNew();
                for (int i = 0; i < totalRows; i++) {
                    empNoVec.setSafe(i, empNoArr[i]);
                    firstNameVec.setSafe(i, firstNames.get(i).getBytes(StandardCharsets.UTF_8));
                    lastNameVec.setSafe(i, lastNames.get(i).getBytes(StandardCharsets.UTF_8));
                    salaryVec.setSafe(i, salaryArr[i]);
                    stillHiredVec.setSafe(i, stillHiredArr[i] ? 1 : 0);
                    heightVec.setSafe(i, heightArr[i]);
                }
                root.setRowCount(totalRows);
                listener.putNext();
                listener.completed();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            server.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing FlightServer", e);
        } finally {
            allocator.close();
        }
    }
}
