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
import org.elasticsearch.xpack.esql.datasources.dataset.GetDatasetAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * {@code FROM DATASETS()} — one row per registered dataset: its name, the data source behind it, the resource it
 * reads and the format it is read as.
 * <p>
 * The relation form of {@code GET _query/dataset}. The same shape as {@link DataSourcesTableFunction} and, for
 * free, a different authorization story: {@code GetDatasetAction.Request} is an indices request, so the security
 * layer <b>narrows</b> it to the names the caller may read rather than refusing the lot. A caller sees their own
 * datasets and no others, and nothing here had to be taught how to decide that — which is the point of
 * dispatching the endpoint's own action rather than reading cluster state and inventing a filter.
 * <p>
 * Two consequences worth knowing. A caller whose role covers no dataset gets no rows rather than an error, which
 * is what the endpoint does. And a dataset covered by document- or field-level security makes the underlying
 * request fail rather than disappear, because the security interceptor refuses that combination instead of
 * narrowing it; that is the endpoint's behaviour too, and the place to change it is the endpoint.
 */
public class DatasetsTableFunction extends CoordinatorRowsFunction {

    public static final String NAME = "datasets";

    private static final TableFunctionSignature SIGNATURE = TableFunctionSignature.of();

    private final Client client;

    public DatasetsTableFunction(Client client) {
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
        return List.of(
            column("name", DataType.KEYWORD),
            column("data_source", DataType.KEYWORD),
            column("resource", DataType.KEYWORD),
            column("format", DataType.KEYWORD),
            column("description", DataType.KEYWORD)
        );
    }

    @Override
    protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
        GetDatasetAction.Request request = new GetDatasetAction.Request(REST_MASTER_TIMEOUT_DEFAULT);
        // "*" is what RestGetDatasetAction sends for a nameless request, and it is these names the security
        // filter narrows. Sending anything else would authorize differently from the endpoint.
        request.indices("*");
        client.execute(GetDatasetAction.INSTANCE, request, listener.map(response -> {
            List<List<Object>> rows = new ArrayList<>(response.getDatasets().size());
            for (var dataset : response.getDatasets()) {
                Object format = dataset.settings() == null ? null : dataset.settings().get("format");
                // Arrays.asList, not List.of: format and description are both optional.
                rows.add(Arrays.asList(dataset.name(), dataset.dataSource().getName(), dataset.resource(), format, dataset.description()));
            }
            return rows;
        }));
    }
}
