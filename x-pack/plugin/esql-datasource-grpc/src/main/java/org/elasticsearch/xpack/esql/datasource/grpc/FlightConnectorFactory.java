/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Factory for Arrow Flight connectors.
 * Handles {@code flight://} and {@code grpc://} URIs.
 */
class FlightConnectorFactory implements ConnectorFactory {

    /** The deadline of the calls that only plan a read (schema, capabilities, discovery, a fragment's flight info). */
    private final TimeValue callTimeout;

    FlightConnectorFactory() {
        this(GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT);
    }

    FlightConnectorFactory(TimeValue callTimeout) {
        this.callTimeout = callTimeout;
    }

    @Override
    public String type() {
        return "flight";
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("flight://") || location.startsWith("grpc://");
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        // A registered dataset's config holds the endpoint and target the validator derived from the resource, plus the
        // schema_resolution the dataset layer always adds. There are no source options yet: the validator refuses dataset settings,
        // and anything else is an unknown option here.
        ConfigKeyValidator.check(config, List.of(Set.of("endpoint", "target", "schema_resolution")));
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
            Schema arrowSchema = client.getSchema(
                FlightDescriptor.path(target),
                CallOptions.timeout(callTimeout.millis(), TimeUnit.MILLISECONDS)
            ).getSchema();
            List<Attribute> attributes = FlightTypeMapping.toAttributes(arrowSchema);
            Map<String, Object> resolvedConfig = Map.of("endpoint", endpoint, "target", target);
            Map<String, Object> stamp = fetchSatelliteCapabilities(client, target, arrowSchema, callTimeout);
            return new SimpleSourceMetadata(attributes, "flight", location, null, null, stamp, resolvedConfig);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while resolving Flight schema for [" + target + "]", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve Flight schema for [" + target + "]", e);
        }
    }

    /** The body of the capability action: the spec versions ES supports and the table being resolved (protocol section 5.2). */
    private static byte[] capabilityRequest(String target) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().array("specVersions", 0).field("target", target).endObject();
            return BytesReference.toBytes(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * One {@code DoAction} during resolution (protocol section 5.2). {@code UNIMPLEMENTED} means a plain Level 0 server and no
     * stamp; any other failure, zero or several results or an unparsable body fails the resolution, so that a transient error does
     * not silently turn pushdown off. The stamp is the declaration plus {@code listColumns}, the columns sent as Arrow lists, and
     * {@code strictColumns}, the columns whose Arrow type the protocol can carry.
     */
    private static Map<String, Object> fetchSatelliteCapabilities(
        FlightClient client,
        String target,
        Schema arrowSchema,
        TimeValue callTimeout
    ) {
        List<Result> results = new ArrayList<>();
        try {
            client.doAction(
                new Action("esql.satellite.capabilities", capabilityRequest(target)),
                CallOptions.timeout(callTimeout.millis(), TimeUnit.MILLISECONDS)
            ).forEachRemaining(results::add);
        } catch (FlightRuntimeException e) {
            if (e.status().code() == FlightStatusCode.UNIMPLEMENTED) {
                return null;
            }
            throw e;
        }
        if (results.size() != 1) {
            throw new IllegalStateException("the capability action answered [" + results.size() + "] results, expected one");
        }
        byte[] body = results.get(0).getBody();
        if (body.length > MAX_DECLARATION_BYTES) {
            throw new IllegalStateException(
                "the capability declaration is [" + body.length + "] bytes, the limit is " + MAX_DECLARATION_BYTES
            );
        }
        Map<String, Object> caps = new HashMap<>(
            XContentHelper.convertToMap(XContentType.JSON.xContent(), new String(body, StandardCharsets.UTF_8), false)
        );
        validateDeclaration(caps);
        List<String> listColumns = new ArrayList<>();
        List<String> strictColumns = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            if (field.getType() instanceof ArrowType.List) {
                listColumns.add(field.getName());
            }
            if (SatelliteSchemaCheck.isStrict(field)) {
                strictColumns.add(field.getName());
            }
        }
        caps.put("listColumns", listColumns);
        // only these columns can appear in a fragment: a satellite refuses any other Arrow type (protocol section 9.2)
        caps.put("strictColumns", strictColumns);
        return Map.of(SatelliteFragment.CAPABILITY_KEY, caps);
    }

    /** Names the protocol reserves for later versions: a declaration that lists one is invalid (section 13). */
    private static final Set<String> RESERVED_FUNCTIONS = Set.of(
        "div",
        "mod",
        "rlike",
        "insensitive_equals",
        "to_upper",
        "to_lower",
        "round",
        "concat",
        "to_long",
        "to_integer",
        "to_double",
        "to_boolean",
        "to_string",
        "to_datetime"
    );

    /** The declaration travels with every plan to every data node, so its size is bounded. */
    private static final int MAX_DECLARATION_BYTES = 64 * 1024;

    /** The checks of section 5.2 that do not need the JSON schema: required properties, limits within bounds, no reserved name. */
    static void validateDeclaration(Map<String, Object> caps) {
        for (String required : List.of("specVersions", "fragmentFormats", "plan", "functions", "types")) {
            if (caps.get(required) instanceof List<?> == false) {
                throw new IllegalStateException("the capability declaration has no list [" + required + "]");
            }
        }
        for (Object function : (List<?>) caps.get("functions")) {
            if (RESERVED_FUNCTIONS.contains(String.valueOf(function))) {
                throw new IllegalStateException("the capability declaration lists the reserved function [" + function + "]");
            }
        }
        if (caps.get("limits") instanceof Map<?, ?> limits) {
            Object depth = limits.get("maxExpressionDepth");
            if (depth instanceof Number n && (n.intValue() < 1 || n.intValue() > 128)) {
                throw new IllegalStateException("maxExpressionDepth [" + depth + "] is outside 1 to 128");
            }
            for (String name : List.of("maxFragmentBytes", "maxInListSize", "maxPatternBytes")) {
                if (limits.get(name) instanceof Number n && n.longValue() < 1) {
                    throw new IllegalStateException(name + " [" + n + "] must be at least 1");
                }
            }
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        // Use Objects.toString — direct (String) cast would CCE on SecureString / non-String values.
        String endpoint = Objects.toString(config.get("endpoint"), null);
        if (endpoint == null) {
            throw new IllegalArgumentException("Flight connector requires 'endpoint' in config");
        }
        return new FlightConnector(endpoint, callTimeout);
    }

    @Override
    public SplitProvider splitProvider() {
        return new FlightSplitProvider(callTimeout);
    }

    private static String extractTarget(URI uri) {
        String path = uri.getPath();
        if (path != null && path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }
}
