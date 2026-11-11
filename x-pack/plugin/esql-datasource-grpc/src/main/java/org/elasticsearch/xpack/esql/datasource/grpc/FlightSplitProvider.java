/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Discovers splits for an Arrow Flight source by calling {@code getInfo()} and
 * mapping each {@link FlightEndpoint} to a {@link FlightSplit}.
 *
 * A single-endpoint response produces one split; multi-endpoint responses
 * produce one split per endpoint, enabling parallel reads across drivers.
 */
class FlightSplitProvider implements SplitProvider {

    private final TimeValue callTimeout;
    private final FlightGuard hosts;

    FlightSplitProvider() {
        this(GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT);
    }

    FlightSplitProvider(TimeValue callTimeout) {
        this(callTimeout, FlightGuard.DEFAULT);
    }

    FlightSplitProvider(TimeValue callTimeout, FlightGuard hosts) {
        this.callTimeout = callTimeout;
        this.hosts = hosts;
    }

    @Override
    public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
        String endpoint = (String) context.config().get("endpoint");
        String target = (String) context.config().get("target");
        if (endpoint == null || target == null) {
            return SplitDiscoveryResult.EMPTY;
        }

        URI uri = URI.create(endpoint);
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        FlightClients.Transport transport = FlightClients.transport(endpoint, context.config());

        try (
            BufferAllocator allocator = FlightClients.planningAllocator(hosts);
            FlightClient client = FlightClients.build(allocator, uri.getHost(), port, transport, hosts)
        ) {
            FlightInfo info = client.getInfo(
                FlightDescriptor.path(target),
                CallOptions.timeout(callTimeout.millis(), TimeUnit.MILLISECONDS)
            );
            List<FlightEndpoint> endpoints = info.getEndpoints();
            if (endpoints.isEmpty()) {
                return SplitDiscoveryResult.EMPTY;
            }
            int maxEndpoints = hosts.limits().maxEndpoints();
            if (endpoints.size() > maxEndpoints) {
                throw new IllegalArgumentException(
                    "the satellite returned ["
                        + endpoints.size()
                        + "] endpoints for ["
                        + target
                        + "], the limit is ["
                        + maxEndpoints
                        + "] (setting ["
                        + GrpcDataSourcePlugin.FLIGHT_MAX_ENDPOINTS.getKey()
                        + "])"
                );
            }

            long totalRecords = info.getRecords();
            long perEndpoint = totalRecords > 0 ? totalRecords / endpoints.size() : -1;

            List<ExternalSplit> splits = new ArrayList<>(endpoints.size());
            for (FlightEndpoint ep : endpoints) {
                byte[] ticketBytes = ep.getTicket().getBytes();
                String loc = ep.getLocations().isEmpty() == false ? ep.getLocations().get(0).getUri().toString() : null;
                splits.add(new FlightSplit(ticketBytes, loc, perEndpoint));
            }
            return SplitDiscoveryResult.of(splits);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during Flight split discovery for [" + target + "]", e);
        } catch (IllegalArgumentException e) {
            throw e; // a satellite that cannot be served: the message says why, and the limit it names is the operator's to change
        } catch (Exception e) {
            IllegalArgumentException rejected = FlightConnector.rejection(e);
            if (rejected != null) {
                throw rejected;
            }
            throw new IllegalStateException("Failed Flight split discovery for [" + target + "]", e);
        }
    }
}
