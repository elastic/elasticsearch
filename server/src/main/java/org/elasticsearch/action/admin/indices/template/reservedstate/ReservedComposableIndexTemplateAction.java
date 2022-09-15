/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.reservedstate;

import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

public class ReservedComposableIndexTemplateAction implements ReservedClusterStateHandler<List<PutComposableIndexTemplateAction.Request>> {
    public static final String NAME = "index_templates";

    private final MetadataIndexTemplateService indexTemplateService;

    public ReservedComposableIndexTemplateAction(MetadataIndexTemplateService indexTemplateService) {
        this.indexTemplateService = indexTemplateService;
    }

    @Override
    public String name() {
        return NAME;
    }

    public List<PutComposableIndexTemplateAction.Request> prepare(List<PutComposableIndexTemplateAction.Request> componentTemplates) {
        for (var componentTemplate : componentTemplates) {
            validate(componentTemplate);
        }

        return componentTemplates;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        var requests = prepare((List<PutComposableIndexTemplateAction.Request>) source);

        ClusterState state = prevState.state();

        for (var request : requests) {
            state = indexTemplateService.addIndexTemplateV2(state, false, request.name(), request.indexTemplate());
        }

        Set<String> entities = requests.stream().map(r -> r.name()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        if (toDelete.isEmpty() == false) {
            state = MetadataIndexTemplateService.innerRemoveIndexTemplateV2(state, toDelete.toArray(String[]::new));
        }

        return new TransformState(state, entities);
    }

    @Override
    public List<PutComposableIndexTemplateAction.Request> fromXContent(XContentParser parser) throws IOException {
        List<PutComposableIndexTemplateAction.Request> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (var entry : source.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) entry.getValue();
            try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                var request = new PutComposableIndexTemplateAction.Request(entry.getKey());
                result.add(request.indexTemplate(ComposableIndexTemplate.parse(componentParser)));
            }
        }

        return result;
    }

    @Override
    public Collection<String> dependencies() {
        return List.of(ReservedComponentTemplateAction.NAME);
    }
}
