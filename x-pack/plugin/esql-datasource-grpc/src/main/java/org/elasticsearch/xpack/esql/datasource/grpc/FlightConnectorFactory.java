/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Factory for Arrow Flight connectors.
 * Handles {@code flight://} and {@code grpc://} URIs.
 */
class FlightConnectorFactory implements ConnectorFactory {

    @Override
    public String type() {
        return "flight";
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("flight://") || location.startsWith("grpc://");
    }

    static final int DEFAULT_FLIGHT_PORT = 47470;

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        URI uri = URI.create(location);
        String target = extractTarget(uri);
        int port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_FLIGHT_PORT;
        String endpoint = uri.getScheme() + "://" + uri.getHost() + ":" + port;
        Location flightLocation = Location.forGrpcInsecure(uri.getHost(), port);

        try (
            BufferAllocator allocator = new RootAllocator();
            FlightClient client = FlightClient.builder(allocator, flightLocation).build()
        ) {
            Schema arrowSchema = client.getSchema(FlightDescriptor.path(target)).getSchema();
            List<Attribute> attributes = FlightTypeMapping.toAttributes(arrowSchema);
            Map<String, Object> resolvedConfig = Map.of("endpoint", endpoint, "target", target);
            return new SimpleSourceMetadata(attributes, "flight", location, null, null, null, resolvedConfig);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while resolving Flight schema for [" + location + "]", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve Flight schema for [" + location + "]: " + e.getMessage(), e);
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        String endpoint = (String) config.get("endpoint");
        if (endpoint == null) {
            throw new IllegalArgumentException("Flight connector requires 'endpoint' in config");
        }
        return new FlightConnector(endpoint);
    }

    private static String extractTarget(URI uri) {
        String path = uri.getPath();
        if (path != null && path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }
}
