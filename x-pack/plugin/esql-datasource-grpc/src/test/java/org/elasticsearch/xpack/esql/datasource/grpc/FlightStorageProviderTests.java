/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class FlightStorageProviderTests extends ESTestCase {

    public void testFlightTargetFromPath() {
        StoragePath path = StoragePath.of("flight://localhost:47470/employees");
        assertEquals("employees", FlightStorageProvider.flightTarget(path));
    }

    public void testFlightTargetNestedPath() {
        StoragePath path = StoragePath.of("flight://localhost:47470/foo/bar");
        assertEquals("foo/bar", FlightStorageProvider.flightTarget(path));
    }

    public void testExistsTrueForKnownFlightDescriptor() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0); StorageProvider provider = new FlightStorageProvider()) {
            StoragePath path = StoragePath.of("flight://localhost:" + server.port() + "/employees");
            assertTrue(provider.exists(path));
            assertTrue(provider.newObject(path).exists());
        }
    }

    public void testExistsThrowsWhenPortNotListening() throws IOException {
        final int freePort;
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            freePort = ss.getLocalPort();
        }
        try (StorageProvider provider = new FlightStorageProvider()) {
            StoragePath path = StoragePath.of("flight://127.0.0.1:" + freePort + "/employees");
            assertThrows(IOException.class, () -> provider.exists(path));
        }
    }

    public void testGrpcSchemeUsesSameProvider() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0); StorageProvider provider = new FlightStorageProvider()) {
            StoragePath path = StoragePath.of("grpc://localhost:" + server.port() + "/employees");
            assertTrue(provider.exists(path));
        }
    }

    public void testListObjectsUnsupported() throws IOException {
        try (StorageProvider provider = new FlightStorageProvider()) {
            StoragePath path = StoragePath.of("flight://localhost:47470/employees");
            assertThrows(UnsupportedOperationException.class, () -> provider.listObjects(path, false));
        }
    }

    public void testNewStreamUnsupported() throws IOException {
        try (StorageProvider provider = new FlightStorageProvider()) {
            StoragePath path = StoragePath.of("flight://localhost:47470/employees");
            assertThrows(IOException.class, () -> provider.newObject(path).newStream());
        }
    }

    public void testRejectsNonFlightScheme() {
        StoragePath path = StoragePath.of("http://localhost:47470/employees");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FlightStorageProvider().newObject(path));
        assertTrue(e.getMessage().contains("flight://"));
    }
}
