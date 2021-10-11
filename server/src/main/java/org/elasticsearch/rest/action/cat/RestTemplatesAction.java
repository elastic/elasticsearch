/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestTemplatesAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_cat/templates"),
            new Route(GET, "/_cat/templates/{name}"));
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
        final String[] templateNames = Strings.splitStringByCommaToArray(request.param("name", ""));

        final GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(templateNames);
        getIndexTemplatesRequest.local(request.paramAsBoolean("local", getIndexTemplatesRequest.local()));
        getIndexTemplatesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexTemplatesRequest.masterNodeTimeout()));

        final GetComposableIndexTemplateAction.Request getComposableTemplatesRequest
            = new GetComposableIndexTemplateAction.Request();
        getComposableTemplatesRequest.local(request.paramAsBoolean("local", getComposableTemplatesRequest.local()));
        getComposableTemplatesRequest.masterNodeTimeout(
            request.paramAsTime("master_timeout", getComposableTemplatesRequest.masterNodeTimeout()));

        return channel -> {

            final StepListener<GetIndexTemplatesResponse> getIndexTemplatesStep = new StepListener<>();
            client.admin().indices().getTemplates(getIndexTemplatesRequest, getIndexTemplatesStep);

            final StepListener<GetComposableIndexTemplateAction.Response> getComposableTemplatesStep = new StepListener<>();
            client.execute(GetComposableIndexTemplateAction.INSTANCE, getComposableTemplatesRequest, getComposableTemplatesStep);

            final ActionListener<Table> tableListener = new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(Table table) throws Exception {
                    return RestTable.buildResponse(table, channel);
                }
            };

            getIndexTemplatesStep.whenComplete(getIndexTemplatesResponse ->
                getComposableTemplatesStep.whenComplete(getComposableIndexTemplatesResponse ->
                    ActionListener.completeWith(tableListener, () -> buildTable(
                        request,
                        getIndexTemplatesResponse,
                        getComposableIndexTemplatesResponse,
                        templateNames)
                    ), tableListener::onFailure
                ), tableListener::onFailure);
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
        GetComposableIndexTemplateAction.Response getComposableIndexTemplatesResponse,
        String[] requestedNames
    ) {
        final Predicate<String> namePredicate = getNamePredicate(requestedNames);

        final Table table = getTableWithHeader(request);
        for (IndexTemplateMetadata indexData : getIndexTemplatesResponse.getIndexTemplates()) {
            assert namePredicate.test(indexData.getName());
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
            if (namePredicate.test(name)) {
                final ComposableIndexTemplate template = entry.getValue();
                table.startRow();
                table.addCell(name);
                table.addCell("[" + String.join(", ", template.indexPatterns()) + "]");
                table.addCell(template.priorityOrZero());
                table.addCell(template.version());
                table.addCell("[" + String.join(", ", template.composedOf()) + "]");
                table.endRow();
            }
        }

        return table;
    }

    private Predicate<String> getNamePredicate(String[] requestedNames) {
        if (requestedNames.length == 0) {
            return name -> true;
        }

        final Set<String> exactMatches = new HashSet<>();
        final List<String> patterns = new ArrayList<>();
        for (String requestedName : requestedNames) {
            if (Regex.isMatchAllPattern(requestedName)) {
                return name -> true;
            } else if (Regex.isSimpleMatchPattern(requestedName)) {
                patterns.add(requestedName);
            } else {
                exactMatches.add(requestedName);
            }
        }

        if (patterns.isEmpty()) {
            return exactMatches::contains;
        }

        return name -> {
            if (exactMatches.contains(name)) {
                return true;
            }
            for (String pattern : patterns) {
                if (Regex.simpleMatch(pattern, name)) {
                    return true;
                }
            }
            return false;
        };
    }
}
