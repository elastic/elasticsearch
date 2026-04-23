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
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Discovers splits for an Arrow Flight source by calling {@code getInfo()} and
 * mapping each {@link FlightEndpoint} to a {@link FlightSplit}.
 *
 * A single-endpoint response produces one split; multi-endpoint responses
 * produce one split per endpoint, enabling parallel reads across drivers.
 */
class FlightSplitProvider implements SplitProvider {

    @Override
    public List<ExternalSplit> discoverSplits(SplitDiscoveryContext context) {
        String endpoint = (String) context.config().get("endpoint");
        String target = (String) context.config().get("target");
        if (endpoint == null || target == null) {
            return List.of();
        }

        URI uri = URI.create(endpoint);
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        Location location = Location.forGrpcInsecure(uri.getHost(), port);

        try (BufferAllocator allocator = new RootAllocator(); FlightClient client = FlightClient.builder(allocator, location).build()) {
            FlightInfo info = client.getInfo(FlightDescriptor.path(target));
            List<FlightEndpoint> endpoints = info.getEndpoints();
            if (endpoints.isEmpty()) {
                return List.of();
            }

            long totalRecords = info.getRecords();
            long perEndpoint = totalRecords > 0 ? totalRecords / endpoints.size() : -1;

            List<ExternalSplit> splits = new ArrayList<>(endpoints.size());
            for (FlightEndpoint ep : endpoints) {
                byte[] ticketBytes = ep.getTicket().getBytes();
                String loc = ep.getLocations().isEmpty() == false ? ep.getLocations().get(0).getUri().toString() : null;
                splits.add(new FlightSplit(ticketBytes, loc, perEndpoint));
            }
            return splits;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during Flight split discovery for [" + endpoint + "/" + target + "]", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed Flight split discovery for [" + endpoint + "/" + target + "]", e);
        }
    }
}
