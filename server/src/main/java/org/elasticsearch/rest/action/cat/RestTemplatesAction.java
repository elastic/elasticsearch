/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
public class RestTemplatesAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/templates"), new Route(GET, "/_cat/templates/{name}"));
    }

    @Override
    public String getName() {
        return "cat_templates_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/templates\n");
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        final String matchPattern = request.hasParam("name") ? request.param("name") : null;

        final GetIndexTemplatesRequest getIndexTemplatesRequest = matchPattern == null
            ? new GetIndexTemplatesRequest()
            : new GetIndexTemplatesRequest(matchPattern);
        getIndexTemplatesRequest.local(request.paramAsBoolean("local", getIndexTemplatesRequest.local()));
        getIndexTemplatesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexTemplatesRequest.masterNodeTimeout()));

        final GetComposableIndexTemplateAction.Request getComposableTemplatesRequest = new GetComposableIndexTemplateAction.Request(
            matchPattern
        );
        getComposableTemplatesRequest.local(request.paramAsBoolean("local", getComposableTemplatesRequest.local()));
        getComposableTemplatesRequest.masterNodeTimeout(
            request.paramAsTime("master_timeout", getComposableTemplatesRequest.masterNodeTimeout())
        );

        return channel -> {

            final ListenableFuture<GetIndexTemplatesResponse> getIndexTemplatesStep = new ListenableFuture<>();
            client.admin().indices().getTemplates(getIndexTemplatesRequest, getIndexTemplatesStep);

            final ListenableFuture<GetComposableIndexTemplateAction.Response> getComposableTemplatesStep = new ListenableFuture<>();
            client.execute(
                GetComposableIndexTemplateAction.INSTANCE,
                getComposableTemplatesRequest,
                getComposableTemplatesStep.delegateResponse((l, e) -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                        l.onResponse(new GetComposableIndexTemplateAction.Response(Collections.emptyMap()));
                    } else {
                        l.onFailure(e);
                    }
                })
            );

            final ActionListener<Table> tableListener = new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(Table table) throws Exception {
                    return RestTable.buildResponse(table, channel);
                }
            };

            getIndexTemplatesStep.addListener(
                tableListener.delegateFailureAndWrap(
                    (l, getIndexTemplatesResponse) -> getComposableTemplatesStep.addListener(
                        l.delegateFailureAndWrap(
                            (ll, getComposableIndexTemplatesResponse) -> ActionListener.completeWith(
                                ll,
                                () -> buildTable(request, getIndexTemplatesResponse, getComposableIndexTemplatesResponse)
                            )
                        )
                    )
                )
            );
        };
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("name", "alias:n;desc:template name");
        table.addCell("index_patterns", "alias:t;desc:template index patterns");
        table.addCell("order", "alias:o,p;desc:template application order/priority number");
        table.addCell("version", "alias:v;desc:version");
        table.addCell("composed_of", "alias:c;desc:component templates comprising index template");
        table.endHeaders();
        return table;
    }

    private Table buildTable(
        RestRequest request,
        GetIndexTemplatesResponse getIndexTemplatesResponse,
        GetComposableIndexTemplateAction.Response getComposableIndexTemplatesResponse
    ) {
        final Table table = getTableWithHeader(request);
        for (IndexTemplateMetadata indexData : getIndexTemplatesResponse.getIndexTemplates()) {
            table.startRow();
            table.addCell(indexData.name());
            table.addCell("[" + String.join(", ", indexData.patterns()) + "]");
            table.addCell(indexData.getOrder());
            table.addCell(indexData.getVersion());
            table.addCell("");
            table.endRow();
        }

        for (Map.Entry<String, ComposableIndexTemplate> entry : getComposableIndexTemplatesResponse.indexTemplates().entrySet()) {
            final String name = entry.getKey();
            final ComposableIndexTemplate template = entry.getValue();
            table.startRow();
            table.addCell(name);
            table.addCell("[" + String.join(", ", template.indexPatterns()) + "]");
            table.addCell(template.priorityOrZero());
            table.addCell(template.version());
            table.addCell("[" + String.join(", ", template.composedOf()) + "]");
            table.endRow();
        }

        return table;
    }
}
