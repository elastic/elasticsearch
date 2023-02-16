/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * cat API class for handling get componentTemplate.
 */
public class RestCatComponentTemplateAction extends AbstractCatAction {
    @Override
    public String getName() {
        return "cat_component_template_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/component_templates"), new Route(GET, "/_cat/component_templates/{name}"));
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/component_templates");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("name", "alias:n;desc:component template name");
        table.addCell("version", "alias:v;desc:version");
        table.addCell("alias_count", "alias:a;desc:alias count");
        table.addCell("mapping_count", "alias:m;desc:mapping count");
        table.addCell("settings_count", "alias:s;desc:settings count");
        table.addCell("metadata_count", "alias:me;desc:metadata count");
        table.addCell("included_in", "alias:i;desc:included in");
        table.endHeaders();
        return table;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        final String matchPattern = request.hasParam("name") ? request.param("name") : null;
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().metadata(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse, matchPattern), channel);
            }
        });
    }

    public Table buildTable(RestRequest request, ClusterStateResponse clusterStateResponse, String patternString) throws Exception {
        Table table = getTableWithHeader(request);
        Metadata metadata = clusterStateResponse.getState().metadata();
        Map<String, Set<String>> reverseIndexOnComposedOfToIndexName = buildReverseIndexOnComposedOfToIndexName(metadata);

        for (Map.Entry<String, ComponentTemplate> entry : metadata.componentTemplates().entrySet()) {
            String name = entry.getKey();
            ComponentTemplate componentTemplate = entry.getValue();
            if (patternString == null || Regex.simpleMatch(patternString, name)) {
                table.startRow();
                table.addCell(name);
                table.addCell(componentTemplate.version());
                Template template = componentTemplate.template();
                table.addCell(template == null || template.aliases() == null ? 0 : template.aliases().size());
                table.addCell(countMappingInTemplate(template));
                table.addCell(template == null || template.settings() == null ? 0 : template.settings().keySet().size());
                table.addCell(componentTemplate.metadata() == null ? 0 : componentTemplate.metadata().size());
                table.addCell(reverseIndexOnComposedOfToIndexName.getOrDefault(name, new HashSet<>()));
                table.endRow();
            }
        }
        return table;
    }

    private static Map<String, Set<String>> buildReverseIndexOnComposedOfToIndexName(Metadata metadata) {
        Map<String, ComposableIndexTemplate> allTemplates = metadata.templatesV2();
        Map<String, Set<String>> reverseIndex = new HashMap<>();

        for (Map.Entry<String, ComposableIndexTemplate> templateEntry : allTemplates.entrySet()) {
            String templateName = templateEntry.getKey();
            ComposableIndexTemplate template = templateEntry.getValue();
            template.composedOf().forEach(composed_of -> reverseIndex.computeIfAbsent(composed_of, k -> new HashSet<>()).add(templateName));
        }
        return reverseIndex;
    }

    private static int countMappingInTemplate(Template template) throws Exception {
        if (template.mappings() == null) {
            return 0;
        }
        int count = 0;
        XContentType xContentType = XContentType.JSON;
        XContentParser parser = xContentType.xContent()
            .createParser(XContentParserConfiguration.EMPTY, template.mappings().uncompressed().array());
        XContentParser.Token token = parser.nextToken();
        String currentFieldName = null;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_doc".equals(currentFieldName)) {
                    List<Object> list = parser.mapOrdered().values().stream().toList();
                    for (Object mapping : list) {
                        count = count + countSubAttributes(mapping);
                    }
                }
            } else {
                parser.skipChildren();
            }
            token = parser.nextToken();
        }
        return count;
    }

    private static int countSubAttributes(Object mapping) {
        int count = 0;
        if (mapping instanceof List) {
            for (Object subObject : (List) mapping) {
                count = count + countSubAttributes(subObject);
            }
        } else if (mapping instanceof Map) {
            for (Object entry : ((Map) mapping).entrySet()) {
                count = count + countSubAttributes(entry);
            }
        } else {
            count = count + 1;
        }
        return count;
    }
}
