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
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.net.URI;

/**
 * Connector for Apache Arrow Flight endpoints.
 * Maintains a persistent {@link FlightClient} connection for the duration of the query.
 */
class FlightConnector implements Connector {

    private final BufferAllocator allocator;
    private final FlightClient client;

    FlightConnector(String endpoint) {
        URI uri = URI.create(endpoint);
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        Location location = Location.forGrpcInsecure(uri.getHost(), port);
        this.allocator = new RootAllocator();
        this.client = FlightClient.builder(allocator, location).build();
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        FlightInfo info = client.getInfo(FlightDescriptor.path(request.target()));
        if (info.getEndpoints().isEmpty()) {
            throw new IllegalStateException("Flight server returned no endpoints for target: " + request.target());
        }
        Ticket ticket = info.getEndpoints().get(0).getTicket();
        FlightStream stream = client.getStream(ticket);
        return new FlightResultCursor(stream, request.attributes(), request.blockFactory());
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing FlightClient", e);
        } finally {
            allocator.close();
        }
    }

    @Override
    public String toString() {
        return "FlightConnector";
    }
}
