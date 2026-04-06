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
import org.elasticsearch.core.IOUtils;
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

    private FlightClient defaultClient;
    private final String defaultEndpoint;
    private final URI endpointURI;
    private final Map<String, FlightClient> locationClients = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    FlightConnector(String endpoint) {
        URI uri = URI.create(endpoint);
        this.endpointURI = uri;
        this.defaultEndpoint = uri.getHost() + ":" + getPort(uri);
    }

    private FlightClient defaultClient(QueryRequest request) {
        if (defaultClient == null) {
            Location location = Location.forGrpcInsecure(endpointURI.getHost(), getPort(endpointURI));
            var allocator = request.blockFactory().arrowAllocator();
            this.defaultClient = FlightClient.builder(allocator, location).build();
        }
        return defaultClient;
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
            client = clientForSplit(flightSplit, request);
        } else {
            client = defaultClient(request);
            FlightInfo info = client.getInfo(FlightDescriptor.path(request.target()));
            if (info.getEndpoints().isEmpty()) {
                throw new IllegalStateException("Flight server returned no endpoints for target: " + request.target());
            }
            ticket = info.getEndpoints().get(0).getTicket();
        }
        FlightStream stream = client.getStream(ticket);
        return new FlightResultCursor(stream, request.attributes(), request.blockFactory());
    }

    private FlightClient clientForSplit(FlightSplit split, QueryRequest request) {
        if (closed) {
            throw new IllegalStateException("Connector is closed");
        }
        String loc = split.location();
        if (loc == null) {
            return defaultClient(request);
        }
        URI uri = URI.create(loc);
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Invalid location URI: missing host: " + loc);
        }
        int port = getPort(uri);
        String key = host + ":" + port;
        if (key.equals(defaultEndpoint)) {
            return defaultClient(request);
        }
        return locationClients.computeIfAbsent(key, k -> {
            Location location = Location.forGrpcInsecure(host, port);
            var allocator = request.blockFactory().arrowAllocator();
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
        IOUtils.close(toClose);
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

    private static int getPort(URI uri) {
        return uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
    }

    @Override
    public String toString() {
        return "FlightConnector";
    }
}
