/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connector for Apache Arrow Flight endpoints.
 * Maintains a default {@link FlightClient} for the configured endpoint and
 * creates additional clients on demand when a {@link FlightSplit} specifies
 * a different location (as returned by multi-endpoint {@code getFlightInfo}).
 */
class FlightConnector implements Connector {

    private final BufferAllocator allocator;
    private final FlightClient defaultClient;
    private final String defaultEndpoint;
    private final Map<String, FlightClient> locationClients = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    FlightConnector(String endpoint) {
        URI uri = URI.create(endpoint);
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        Location location = Location.forGrpcInsecure(uri.getHost(), port);
        this.allocator = new RootAllocator();
        this.defaultClient = FlightClient.builder(allocator, location).build();
        this.defaultEndpoint = uri.getHost() + ":" + port;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        return doExecute(request, null);
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return doExecute(request, split instanceof FlightSplit fs ? fs : null);
    }

    private ResultCursor doExecute(QueryRequest request, FlightSplit flightSplit) {
        Ticket ticket;
        FlightClient client;
        if (flightSplit != null) {
            ticket = new Ticket(flightSplit.ticketBytes());
            client = clientForSplit(flightSplit);
        } else {
            client = defaultClient;
            FlightInfo info = client.getInfo(FlightDescriptor.path(request.target()));
            if (info.getEndpoints().isEmpty()) {
                throw new IllegalStateException("Flight server returned no endpoints for target: " + request.target());
            }
            ticket = info.getEndpoints().get(0).getTicket();
        }
        FlightStream stream = client.getStream(ticket);
        return new FlightResultCursor(stream, request.attributes(), request.blockFactory());
    }

    private FlightClient clientForSplit(FlightSplit split) {
        if (closed) {
            throw new IllegalStateException("Connector is closed");
        }
        String loc = split.location();
        if (loc == null) {
            return defaultClient;
        }
        URI uri = URI.create(loc);
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Invalid location URI: missing host: " + loc);
        }
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        String key = host + ":" + port;
        if (key.equals(defaultEndpoint)) {
            return defaultClient;
        }
        return locationClients.computeIfAbsent(key, k -> {
            Location location = Location.forGrpcInsecure(host, port);
            return FlightClient.builder(allocator, location).build();
        });
    }

    @Override
    public void close() throws IOException {
        closed = true;
        List<Closeable> toClose = new ArrayList<>(locationClients.size() + 1);
        for (FlightClient client : locationClients.values()) {
            toClose.add(asCloseable(client));
        }
        locationClients.clear();
        toClose.add(asCloseable(defaultClient));
        try {
            org.elasticsearch.core.IOUtils.close(toClose);
        } finally {
            allocator.close();
        }
    }

    private static Closeable asCloseable(FlightClient client) {
        return () -> {
            try {
                client.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while closing FlightClient", e);
            }
        };
    }

    @Override
    public String toString() {
        return "FlightConnector";
    }
}
