/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Registers {@code flight} data sources and datasets, so that a satellite (or any Flight server) can be queried as
 * {@code FROM <dataset>}. A data source's only settings are for TLS and authentication: {@code ca_certificate} (PEM text),
 * {@code server_name} and {@code token} (a bearer token, the only secret). They apply to datasets whose resource uses the
 * {@code flight+tls} or {@code grpc+tls} scheme. A dataset's resource is {@code flight://host:port/table}; the validator
 * derives the {@code endpoint} and {@code target} the connector needs. The dataset's
 * settings are source options for the satellite: scalars only, and checked against the satellite's declaration when a query is
 * resolved. Options only change values, never the schema or the partitions (protocol section 11).
 */
class FlightDataSourceValidator implements DataSourceValidator {

    private final FlightGuard hosts;

    FlightDataSourceValidator() {
        this(FlightGuard.DEFAULT);
    }

    FlightDataSourceValidator(FlightGuard hosts) {
        this.hosts = hosts;
    }

    private static final Set<String> SCHEMES = Set.of("flight", "grpc", "flight+tls", "grpc+tls");

    @Override
    public String type() {
        return "flight";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        Map<String, DataSourceSetting> validated = new HashMap<>();
        if (datasourceSettings != null) {
            for (Map.Entry<String, Object> setting : datasourceSettings.entrySet()) {
                if (FlightClients.TLS_SETTINGS.contains(setting.getKey()) == false) {
                    throw error(
                        "a flight data source takes the settings "
                            + new TreeSet<>(FlightClients.TLS_SETTINGS)
                            + ", got ["
                            + setting.getKey()
                            + "]"
                    );
                }
                if (setting.getValue() instanceof String == false || ((String) setting.getValue()).isBlank()) {
                    throw error("[" + setting.getKey() + "] must be a non-empty string");
                }
                if (FlightClients.CA_CERTIFICATE.equals(setting.getKey())) {
                    try {
                        FlightClients.checkCertificates((String) setting.getValue());
                    } catch (IllegalArgumentException e) {
                        throw error(e.getMessage());
                    }
                }
                if (FlightClients.TOKEN.equals(setting.getKey()) && ((String) setting.getValue()).matches("[!-~]+") == false) {
                    throw error("[token] must be printable ASCII characters without spaces");
                }
                if (FlightClients.TOKEN.equals(setting.getKey())
                    && ((String) setting.getValue()).length() > FlightClients.MAX_TOKEN_CHARS) {
                    throw error("[token] is longer than " + FlightClients.MAX_TOKEN_CHARS + " characters");
                }
                if (FlightClients.CREDENTIALS.equals(setting.getKey())) {
                    checkCredentials((String) setting.getValue());
                }
                validated.put(
                    setting.getKey(),
                    new DataSourceSetting(setting.getValue(), FlightClients.SECRET_SETTINGS.contains(setting.getKey()))
                );
            }
        }
        return validated;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        URI uri;
        try {
            uri = URI.create(resource);
        } catch (IllegalArgumentException e) {
            throw error("invalid flight resource [" + resource + "]");
        }
        if (uri.getScheme() == null || SCHEMES.contains(uri.getScheme()) == false || uri.getHost() == null) {
            throw error("a flight resource must look like flight://host:port/table, got [" + resource + "]");
        }
        if (FlightClients.isTls(uri.getScheme()) == false && datasourceSettings != null) {
            for (String name : FlightClients.TLS_SETTINGS) {
                if (datasourceSettings.containsKey(name)) {
                    throw error(
                        "the data source sets [" + name + "], which needs a flight+tls or grpc+tls resource, got [" + resource + "]"
                    );
                }
            }
        }
        try {
            hosts.check(uri.getHost());
        } catch (IllegalArgumentException e) {
            throw error(e.getMessage());
        }
        String path = uri.getPath();
        if (path == null || path.length() < 2 || path.indexOf('/', 1) >= 0) {
            throw error("a flight resource names exactly one table, got [" + resource + "]");
        }
        Map<String, Object> validated = new HashMap<>();
        if (datasetSettings != null) {
            for (Map.Entry<String, Object> setting : datasetSettings.entrySet()) {
                String name = setting.getKey();
                Object v = setting.getValue();
                if (FlightConnectorFactory.NON_OPTION_KEYS.contains(name) || name.startsWith("_")) {
                    throw error("[" + name + "] is not a source option: it is set by ES");
                }
                if (v instanceof String == false && v instanceof Number == false && v instanceof Boolean == false) {
                    throw error("the source option [" + name + "] must be a string, a number or a boolean");
                }
                validated.put(name, v);
            }
        }
        String header = FlightClients.encodeDatasetSettings(validated);
        if (header != null && header.length() > FlightClients.MAX_HEADER_VALUE_CHARS) {
            throw error("the dataset settings are too large to be sent to the satellite in a header");
        }
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        validated.put("endpoint", uri.getScheme() + "://" + uri.getHost() + ":" + port);
        validated.put("target", path.substring(1));
        return validated;
    }

    /** The credentials are an opaque JSON object for the satellite; ES only checks that it is one and that it fits in a header. */
    private static void checkCredentials(String credentials) {
        if (credentials.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 4 / 3 + 4 > FlightClients.MAX_HEADER_VALUE_CHARS) {
            throw error("[credentials] is too long to be sent to the satellite in a header");
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, credentials)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw error("[credentials] must be a JSON object");
            }
            parser.map();
            if (parser.nextToken() != null) {
                throw error("[credentials] must be a single JSON object");
            }
        } catch (IOException | RuntimeException e) {
            if (e instanceof ValidationException ve) {
                throw ve;
            }
            throw error("[credentials] must be a JSON object");
        }
    }

    private static ValidationException error(String message) {
        ValidationException e = new ValidationException();
        e.addValidationError(message);
        return e;
    }
}
