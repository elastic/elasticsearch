/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registers {@code flight} data sources and datasets, so that a satellite (or any Flight server) can be queried as
 * {@code FROM <dataset>}. A data source carries no settings in v0 (no credentials, no TLS). A dataset's resource is
 * {@code flight://host:port/table}; the validator derives the {@code endpoint} and {@code target} the connector needs. Dataset
 * settings are refused: source options have no way to reach a plain scan yet.
 */
class FlightDataSourceValidator implements DataSourceValidator {

    private static final Set<String> SCHEMES = Set.of("flight", "grpc");

    @Override
    public String type() {
        return "flight";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (datasourceSettings != null && datasourceSettings.isEmpty() == false) {
            throw error("a flight data source takes no settings, got " + datasourceSettings.keySet());
        }
        return Map.of();
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
        String path = uri.getPath();
        if (path == null || path.length() < 2 || path.indexOf('/', 1) >= 0) {
            throw error("a flight resource names exactly one table, got [" + resource + "]");
        }
        if (datasetSettings != null && datasetSettings.isEmpty() == false) {
            // source options reach a satellite only inside a fragment, which a plain scan never sends, so they would silently
            // change results depending on whether a query is pushed; they are refused until that is designed
            throw error("a flight dataset takes no settings yet, got " + datasetSettings.keySet());
        }
        Map<String, Object> validated = new HashMap<>();
        int port = uri.getPort() > 0 ? uri.getPort() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        validated.put("endpoint", uri.getScheme() + "://" + uri.getHost() + ":" + port);
        validated.put("target", path.substring(1));
        return validated;
    }

    private static ValidationException error(String message) {
        ValidationException e = new ValidationException();
        e.addValidationError(message);
        return e;
    }
}
