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
import java.util.TreeMap;
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
        // A registered dataset's config holds the endpoint and target the validator derived from the resource, the
        // schema_resolution the dataset layer always adds, and the dataset's own settings, which are source options. Whether the
        // satellite understands an option is only known once its declaration has been read, so that is checked in resolveMetadata.
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (NON_OPTION_KEYS.contains(entry.getKey()) == false
                && entry.getKey().startsWith("_") == false
                && isScalar(entry.getValue()) == false) {
                throw new IllegalArgumentException("the source option [" + entry.getKey() + "] must be a string, a number or a boolean");
            }
        }
    }

    static final int DEFAULT_FLIGHT_PORT = 47470;

    /** Config keys that are not source options: what the validator derives from the resource and what the dataset layer adds. */
    static final Set<String> NON_OPTION_KEYS = SatelliteFragment.NON_OPTION_KEYS;

    private static boolean isScalar(Object value) {
        return value instanceof String || value instanceof Number || value instanceof Boolean;
    }

    /** The source options of a config: every key that is neither one of ours nor a framework key (those start with an underscore). */
    static Map<String, Object> optionsOf(Map<String, Object> config) {
        Map<String, Object> options = new TreeMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (SatelliteFragment.isOptionKey(entry.getKey())) {
                options.put(entry.getKey(), entry.getValue());
            }
        }
        return options;
    }

    /**
     * Options travel only inside a fragment, so they need a satellite that declared them (protocol section 11). A plain Flight
     * server cannot take them, and an option the satellite did not declare would be silently ignored: both fail the query.
     */
    static void checkOptions(Map<String, Object> options, Map<String, Object> stamp) {
        if (options.isEmpty()) {
            return;
        }
        if (stamp == null) {
            throw new IllegalArgumentException(
                "the dataset sets the source options "
                    + options.keySet()
                    + " but this Flight server does not implement the satellite protocol"
            );
        }
        Object declared = ((Map<?, ?>) stamp.get(SatelliteFragment.CAPABILITY_KEY)).get("options");
        for (String name : options.keySet()) {
            if (declared instanceof List<?> list == false || ((List<?>) declared).contains(name) == false) {
                throw new IllegalArgumentException(
                    "the satellite does not declare the source option ["
                        + name
                        + "]; it declares "
                        + (declared instanceof List<?> ? declared : List.of())
                );
            }
        }
    }

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
            checkOptions(optionsOf(config), stamp);
            return new SimpleSourceMetadata(attributes, "flight", location, null, null, stamp, resolvedConfig);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while resolving Flight schema for [" + target + "]", e);
        } catch (IllegalArgumentException e) {
            throw e; // a dataset that cannot be served: the message says why, and it is the user's to fix
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve Flight schema for [" + target + "]", e);
        }
    }

    /** The body of the capability action: the spec versions ES supports and the table being resolved (protocol section 5.2). */
    private static byte[] capabilityRequest(String target) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject()
                .array("specVersions", SatelliteFragment.SUPPORTED_SPEC_VERSIONS.stream().mapToInt(Integer::intValue).toArray())
                .field("target", target)
                .endObject();
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
        checkSpecVersions(caps);
        int version = negotiatedVersion(caps);
        if (version < 0) {
            // shaped for versions ES does not know, so it may hold names and shapes ES cannot judge: a plain Level 0 server
            // (protocol section 5.2)
            return null;
        }
        validateDeclaration(caps);
        if (version >= 1 && caps.get("aggregates") instanceof List<?> aggregates && aggregates.contains("sum")) {
            // ES pushes sum(integer) only when the table's record count proves that no accumulator can overflow (protocol section 8.7)
            caps.put(
                "totalRecords",
                client.getInfo(FlightDescriptor.path(target), CallOptions.timeout(callTimeout.millis(), TimeUnit.MILLISECONDS)).getRecords()
            );
        }
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
        "to_datetime",
        "count",
        "sum",
        "min",
        "max",
        "avg",
        "bucket"
    );

    /** Aggregate functions that version 1 reserves for later versions (section 13). */
    private static final Set<String> RESERVED_AGGREGATES = Set.of(
        "avg",
        "count_distinct",
        "median",
        "median_absolute_deviation",
        "percentile",
        "std_dev",
        "variance",
        "values",
        "top",
        "first",
        "last",
        "sample"
    );

    /** The declaration travels with every plan to every data node, so its size is bounded. */
    private static final int MAX_DECLARATION_BYTES = 64 * 1024;

    /** {@code specVersions} must be a non-empty array of integers for the declaration to satisfy the schema. */
    static void checkSpecVersions(Map<String, Object> caps) {
        if (caps.get("specVersions") instanceof List<?> versions && versions.isEmpty() == false) {
            for (Object version : versions) {
                boolean integer = version instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue());
                if (integer == false) {
                    throw new IllegalStateException("the capability declaration lists the spec version [" + version + "], not an integer");
                }
            }
            return;
        }
        throw new IllegalStateException("the capability declaration has no non-empty list [specVersions]");
    }

    /** The highest spec version both ES and the declaration list, or -1: the declaration is shaped for it (protocol section 4). */
    static int negotiatedVersion(Map<String, Object> caps) {
        int highest = -1;
        if (caps.get("specVersions") instanceof List<?> versions) {
            for (Object v : versions) {
                if (v instanceof Number n && SatelliteFragment.SUPPORTED_SPEC_VERSIONS.contains(n.intValue())) {
                    highest = Math.max(highest, n.intValue());
                }
            }
        }
        return highest;
    }

    static void validateDeclaration(Map<String, Object> caps) {
        checkSpecVersions(caps);
        for (String required : List.of("specVersions", "fragmentFormats", "plan", "functions", "types")) {
            if (caps.get(required) instanceof List<?> == false) {
                throw new IllegalStateException("the capability declaration has no list [" + required + "]");
            }
        }
        if (caps.get("options") instanceof List<?> declaredOptions) {
            for (Object option : declaredOptions) {
                if (SatelliteFragment.isOptionKey(String.valueOf(option)) == false) {
                    throw new IllegalStateException(
                        "the capability declaration lists the option [" + option + "], a name ES uses for itself"
                    );
                }
            }
        }
        boolean aggregateOperator = ((List<?>) caps.get("plan")).contains("aggregate");
        if (negotiatedVersion(caps) == 0) {
            // the declaration is shaped for version 0, where the operator and the property are reserved (protocol section 4)
            if (aggregateOperator || caps.containsKey("aggregates")) {
                throw new IllegalStateException(
                    "the capability declaration of version 0 lists the reserved operator or property [aggregate]"
                );
            }
        } else if (caps.get("aggregates") instanceof List<?> aggregates) {
            for (Object name : aggregates) {
                if (RESERVED_AGGREGATES.contains(String.valueOf(name))) {
                    throw new IllegalStateException("the capability declaration lists the reserved aggregate function [" + name + "]");
                }
            }
            if (aggregates.isEmpty() == false && aggregateOperator == false) {
                throw new IllegalStateException("the capability declaration lists aggregate functions but not the operator [aggregate]");
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
            // so that a fragment, its echoed descriptor and a self-contained ticket fit a 4 MiB gRPC message (protocol section 6)
            if (limits.get("maxFragmentBytes") instanceof Number n && (n.longValue() < 1024 || n.longValue() > 2 * 1024 * 1024)) {
                throw new IllegalStateException("maxFragmentBytes [" + n + "] is outside 1024 to 2097152");
            }
            // ES reads them into an int (protocol section 6)
            for (String name : List.of("maxInListSize", "maxPatternBytes")) {
                if (limits.get(name) instanceof Number n && (n.longValue() < 1 || n.longValue() > Integer.MAX_VALUE)) {
                    throw new IllegalStateException(name + " [" + n + "] is outside 1 to " + Integer.MAX_VALUE);
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
