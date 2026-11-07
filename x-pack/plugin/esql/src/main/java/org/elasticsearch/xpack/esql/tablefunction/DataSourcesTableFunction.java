/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.datasource.GetDataSourceAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * {@code FROM DATA_SOURCES()} — one row per registered data source.
 * <p>
 * The relation form of {@code GET _query/data_source}, and the whole language applies to it:
 * {@code FROM DATA_SOURCES() | WHERE type == "s3" | STATS COUNT(*)} is not something the endpoint can answer.
 *
 * <h2>Authorization, and why there is no new notion of it here</h2>
 *
 * This dispatches the same action the endpoint dispatches, with the same {@code "*"} the endpoint sends when no
 * name is given, in the caller's own thread context. So the security layer decides exactly what it decides for
 * {@code GET _query/data_source}: a caller the endpoint would refuse is refused here, with the same error, and one
 * it would answer gets the same answer.
 * <p>
 * That is deliberately not the function authorizing anything. It names no privilege, reads no role, and makes no
 * decision — it asks an existing API, which authorizes. A function that invented its own rule here could be
 * stricter or looser than the endpoint it mirrors, and nothing would notice; mirroring makes the two impossible
 * to drift apart.
 * <p>
 * Settings are not a column. A data source's settings are where its credentials live, and a relation printing
 * them would be a credential-reading API wearing a different name. Name, type and description are what a reader
 * needs; the registration API owns the rest.
 */
public class DataSourcesTableFunction extends CoordinatorRowsFunction {

    public static final String NAME = "data_sources";

    private static final TableFunctionSignature SIGNATURE = TableFunctionSignature.of();

    private final Client client;

    public DataSourcesTableFunction(Client client) {
        this.client = client;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TableFunctionSignature signature() {
        return SIGNATURE;
    }

    @Override
    protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
        return List.of(column("name", DataType.KEYWORD), column("type", DataType.KEYWORD), column("description", DataType.KEYWORD));
    }

    @Override
    protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
        // "*" is what RestGetDataSourceAction sends for a nameless request. Not a pattern this function invented:
        // the privilege is evaluated against these names, so sending anything else would authorize differently
        // from the endpoint.
        GetDataSourceAction.Request request = new GetDataSourceAction.Request(REST_MASTER_TIMEOUT_DEFAULT, new String[] { "*" });
        client.execute(GetDataSourceAction.INSTANCE, request, listener.map(response -> {
            List<List<Object>> rows = new ArrayList<>(response.getDataSources().size());
            for (var dataSource : response.getDataSources()) {
                // Arrays.asList, not List.of: a description is optional and List.of refuses a null element.
                rows.add(Arrays.asList(dataSource.name(), dataSource.type(), dataSource.description()));
            }
            return rows;
        }));
    }
}
