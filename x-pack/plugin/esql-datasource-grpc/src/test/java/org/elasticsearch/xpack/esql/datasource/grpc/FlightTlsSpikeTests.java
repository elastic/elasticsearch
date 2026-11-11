/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** Spike: does TLS and a bearer token header work with Arrow Flight inside this module's classpath? */
public class FlightTlsSpikeTests extends ESTestCase {

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = FlightTlsSpikeTests.class.getResourceAsStream("tls/" + name)) {
            return in.readAllBytes();
        }
    }

    private static final FlightServerMiddleware.Key<FlightServerMiddleware> KEY = FlightServerMiddleware.Key.of("bearer");

    /** Rejects a call without the expected bearer token. */
    private static final FlightServerMiddleware.Factory<FlightServerMiddleware> BEARER = new FlightServerMiddleware.Factory<>() {
        @Override
        public FlightServerMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
            String header = incomingHeaders.get("authorization");
            if (header == null) {
                throw CallStatus.UNAUTHENTICATED.withDescription("no token").toRuntimeException();
            }
            if (header.equals("Bearer s3cret") == false) {
                throw CallStatus.UNAUTHENTICATED.withDescription("bad token").toRuntimeException();
            }
            return new FlightServerMiddleware() {
                @Override
                public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

                @Override
                public void onCallCompleted(CallStatus status) {}

                @Override
                public void onCallErrored(Throwable err) {}
            };
        }
    };

    private FlightServer server(BufferAllocator allocator) throws Exception {
        NoOpFlightProducer producer = new NoOpFlightProducer() {
            @Override
            public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
                return new SchemaResult(BlockingSatelliteServer.SCHEMA);
            }
        };
        FlightServer server = FlightServer.builder(allocator, Location.forGrpcTls("127.0.0.1", 0), producer)
            .useTls(new ByteArrayInputStream(resource("server.crt")), new ByteArrayInputStream(resource("server.key")))
            .middleware(KEY, BEARER)
            .build();
        server.start();
        return server;
    }

    /** One middleware adds the token to every call of the client: getSchema, getFlightInfo, doAction and getStream alike. */
    private static FlightClientMiddleware.Factory bearer(String token) {
        return info -> new FlightClientMiddleware() {
            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert("authorization", "Bearer " + token);
            }

            @Override
            public void onHeadersReceived(CallHeaders incomingHeaders) {}

            @Override
            public void onCallCompleted(CallStatus status) {}
        };
    }

    private FlightClient client(BufferAllocator allocator, int port, byte[] trusted, String token) throws Exception {
        try (InputStream ca = new ByteArrayInputStream(trusted)) {
            FlightClient.Builder builder = FlightClient.builder(allocator, Location.forGrpcTls("127.0.0.1", port)).trustedCertificates(ca);
            if (token != null) {
                builder.intercept(bearer(token));
            }
            return builder.build();
        }
    }

    public void testTlsAndBearerTokenWork() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(); FlightServer server = server(allocator)) {
            try (FlightClient client = client(allocator, server.getPort(), resource("ca.crt"), "s3cret")) {
                assertEquals(1, client.getSchema(FlightDescriptor.path("t")).getSchema().getFields().size());
            }
            try (FlightClient client = client(allocator, server.getPort(), resource("ca.crt"), null)) {
                FlightRuntimeException noToken = expectThrows(
                    FlightRuntimeException.class,
                    () -> client.getSchema(FlightDescriptor.path("t"))
                );
                assertEquals(FlightStatusCode.UNAUTHENTICATED, noToken.status().code());
            }
            try (FlightClient client = client(allocator, server.getPort(), resource("ca.crt"), "wrong")) {
                FlightRuntimeException bad = expectThrows(FlightRuntimeException.class, () -> client.getSchema(FlightDescriptor.path("t")));
                assertEquals(FlightStatusCode.UNAUTHENTICATED, bad.status().code());
            }
        }
    }

    public void testAnUntrustedCertificateFailsTheHandshake() throws Exception {
        // a certificate the client does not trust: the server's own certificate is not a CA for itself in the chain sense, but a
        // different CA is certainly not trusted
        byte[] other = new String(resource("ca.crt"), StandardCharsets.UTF_8).replace("A", "B").getBytes(StandardCharsets.UTF_8);
        try (BufferAllocator allocator = new RootAllocator(); FlightServer server = server(allocator)) {
            expectThrows(Exception.class, () -> {
                try (FlightClient client = client(allocator, server.getPort(), other, "s3cret")) {
                    client.getSchema(FlightDescriptor.path("t"));
                }
            });
        }
    }
}
