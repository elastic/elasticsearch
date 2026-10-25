/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Collectors;

/** Selects only required columns; ES|QL evaluates filters, sorting, and aggregates locally. */
final class ClickHouseConnector implements Connector {
    private final ClickHouseClient client;

    ClickHouseConnector(ClickHouseClient client) {
        this.client = client;
    }

    static String query(QueryRequest request) {
        String projection = request.attributes().isEmpty()
            ? "1"
            : request.attributes().stream().map(ClickHouseConnector::column).collect(Collectors.joining(", "));
        return "SELECT " + projection + " FROM " + ClickHouseTable.parse(request.target()).sql();
    }

    private static String column(Attribute attribute) {
        String name = ClickHouseTable.quote(attribute.name());
        if (attribute.dataType() == DataType.DATETIME) {
            return "toUnixTimestamp64Milli(toDateTime64(" + name + ", 3, 'UTC')) AS " + name;
        }
        if (attribute.dataType() == DataType.KEYWORD) {
            return "toString(" + name + ") AS " + name;
        }
        return name;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        try {
            return new ClickHouseResultCursor(client.query(query(request)), request);
        } catch (IOException e) {
            client.close();
            throw new UncheckedIOException("Cannot query ClickHouse", e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
