/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.reservedstate;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

public class ReservedComponentTemplateAction implements ReservedClusterStateHandler<List<PutComponentTemplateAction.Request>> {
    public static final String NAME = "component_templates";

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;

    public ReservedComponentTemplateAction(MetadataIndexTemplateService indexTemplateService, IndexScopedSettings indexScopedSettings) {
        this.indexScopedSettings = indexScopedSettings;
        this.indexTemplateService = indexTemplateService;
    }

    @Override
    public String name() {
        return NAME;
    }

    public List<PutComponentTemplateAction.Request> prepare(List<PutComponentTemplateAction.Request> componentTemplates) {
        for (var componentTemplate : componentTemplates) {
            validate(componentTemplate);
        }

        return componentTemplates;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        var requests = prepare((List<PutComponentTemplateAction.Request>) source);

        ClusterState state = prevState.state();

        for (var request : requests) {
            ComponentTemplate template = TransportPutComponentTemplateAction.normalizeComponentTemplate(
                request.componentTemplate(),
                indexScopedSettings
            );

            state = indexTemplateService.addComponentTemplate(state, false, request.name(), template);
        }

        Set<String> entities = requests.stream().map(r -> r.name()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        if (toDelete.isEmpty() == false) {
            state = MetadataIndexTemplateService.innerRemoveComponentTemplate(state, toDelete.toArray(String[]::new));
        }

        return new TransformState(state, entities);
    }

    @Override
    public List<PutComponentTemplateAction.Request> fromXContent(XContentParser parser) throws IOException {
        List<PutComponentTemplateAction.Request> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (var entry : source.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) entry.getValue();
            try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                var request = new PutComponentTemplateAction.Request(entry.getKey());
                result.add(request.componentTemplate(ComponentTemplate.parse(componentParser)));
            }
        }

        return result;
    }
}
