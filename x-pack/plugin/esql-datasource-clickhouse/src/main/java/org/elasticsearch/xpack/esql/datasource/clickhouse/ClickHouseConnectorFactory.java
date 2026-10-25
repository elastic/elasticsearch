/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Discovers table schemas without fetching rows and opens per-query connections. */
final class ClickHouseConnectorFactory implements ConnectorFactory {
    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("clickhouse://");
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ClickHouseTable.parse(location);
        ConfigKeyValidator.check(config, List.of(ClickHouseConfiguration.FIELDS.keySet(), ExternalSourceResolver.CONFIG_KEYS));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        ClickHouseTable table = ClickHouseTable.parse(location);
        try (
            ClickHouseClient client = new ClickHouseClient(config);
            ClickHouseClient.Response response = client.query("SELECT * FROM " + table.sql() + " LIMIT 0")
        ) {
            List<Object> names = response.readRow();
            List<Object> types = response.readRow();
            if (names == null || types == null || names.isEmpty() || names.size() != types.size() || names.size() > 1024) {
                throw new IOException("ClickHouse returned an invalid schema or more than 1024 columns");
            }
            List<Attribute> attributes = new ArrayList<>(names.size());
            for (int column = 0; column < names.size(); column++) {
                if (names.get(column) instanceof String name && types.get(column) instanceof String type) {
                    attributes.add(
                        new ReferenceAttribute(Source.EMPTY, null, name, ClickHouseTypes.dataType(type), Nullability.TRUE, null, false)
                    );
                } else {
                    throw new IOException("ClickHouse returned an invalid schema header");
                }
            }
            if (response.readRow() != null) {
                throw new IOException("ClickHouse returned rows for a schema-only query");
            }
            return new SimpleSourceMetadata(attributes, type(), location);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot resolve the ClickHouse table schema", e);
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        return new ClickHouseConnector(new ClickHouseClient(config));
    }

    @Override
    public void testConnection(Map<String, Object> config) throws IOException {
        try (ClickHouseClient client = new ClickHouseClient(config); ClickHouseClient.Response response = client.query("SELECT 1")) {
            if (response.readRow() == null
                || response.readRow() == null
                || List.of(1).equals(response.readRow()) == false
                || response.readRow() != null) {
                throw new IOException("ClickHouse connection probe returned an invalid result");
            }
        }
    }
}
