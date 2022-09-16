/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.reservedstate;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

public class ReservedComposableIndexTemplateAction
    implements ReservedClusterStateHandler<ReservedComposableIndexTemplateAction.ComponentsAndComposables> {
    public static final String NAME = "index_templates";
    public static final String COMPONENTS = "component_templates";
    private static final String COMPONENT_PREFIX = "component_template:";
    public static final String COMPOSABLES = "composable_index_templates";
    public static final String COMPOSABLE_PREFIX = "composable_index_template:";

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;

    public ReservedComposableIndexTemplateAction(
        MetadataIndexTemplateService indexTemplateService,
        IndexScopedSettings indexScopedSettings
    ) {
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    public String name() {
        return NAME;
    }

    public static String componentName(String name) {
        return COMPONENT_PREFIX + name;
    }

    public static String composableIndexName(String name) {
        return COMPOSABLE_PREFIX + name;
    }

    private Tuple<List<PutComponentTemplateAction.Request>, List<PutComposableIndexTemplateAction.Request>> prepare(
        ComponentsAndComposables componentsAndComposables
    ) {
        List<PutComponentTemplateAction.Request> components = new ArrayList<>();
        List<PutComposableIndexTemplateAction.Request> composables = new ArrayList<>();

        for (var entry : componentsAndComposables.componentTemplates.entrySet()) {
            var request = new PutComponentTemplateAction.Request(entry.getKey());
            request.componentTemplate(entry.getValue());
            validate(request);
            components.add(request);
        }

        for (var entry : componentsAndComposables.composableTemplates.entrySet()) {
            var request = new PutComposableIndexTemplateAction.Request(entry.getKey());
            request.indexTemplate(entry.getValue());
            validate(request);
            composables.add(request);
        }

        return new Tuple<>(components, composables);
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        var requests = prepare((ComponentsAndComposables) source);
        ClusterState state = prevState.state();

        // We transform in the following order:
        //    1. create or update component templates (composable templates depend on them)
        //    2. create or update composable index templates (with disabled v2 overlap validation, we might delete some at step 3,
        //       while, 2 and 3 cannot be reversed because of data streams)
        //    3. delete composable index templates (this will fail on attached data streams, unless we added higher priority one)
        //    4. validate for v2 composable template overlaps
        //    5. delete component templates (this will check if there are any related composable index templates and fail)

        var components = requests.v1();
        var composables = requests.v2();

        // 1. create or update component templates (composable templates depend on them)
        for (var request : components) {
            ComponentTemplate template = TransportPutComponentTemplateAction.normalizeComponentTemplate(
                request.componentTemplate(),
                indexScopedSettings
            );

            state = indexTemplateService.addComponentTemplate(state, false, request.name(), template);
        }

        // 2. create or update composable index templates, no overlap validation
        for (var request : composables) {
            state = indexTemplateService.addIndexTemplateV2(state, false, request.name(), request.indexTemplate(), false);
        }

        Set<String> composableEntities = composables.stream().map(r -> composableIndexName(r.name())).collect(Collectors.toSet());
        Set<String> composablesToDelete = new HashSet<>(
            prevState.keys().stream().filter(k -> k.startsWith(COMPOSABLE_PREFIX)).collect(Collectors.toSet())
        );
        composablesToDelete.removeAll(composableEntities);

        // 3. delete composable index templates (this will fail on attached data streams, unless we added a higher priority one)
        if (composablesToDelete.isEmpty() == false) {
            state = MetadataIndexTemplateService.innerRemoveIndexTemplateV2(state, composablesToDelete.toArray(String[]::new));
        }

        // 4. validate for v2 composable template overlaps
        for (var request : composables) {
            indexTemplateService.v2TemplateOverlaps(state, request.name(), request.indexTemplate(), true);
        }

        Set<String> componentEntities = components.stream().map(r -> componentName(r.name())).collect(Collectors.toSet());
        Set<String> componentsToDelete = new HashSet<>(
            prevState.keys().stream().filter(k -> k.startsWith(COMPONENT_PREFIX)).collect(Collectors.toSet())
        );
        componentsToDelete.removeAll(componentEntities);

        // 5. delete component templates (this will check if there are any related composable index templates and fail)
        if (componentsToDelete.isEmpty() == false) {
            state = MetadataIndexTemplateService.innerRemoveComponentTemplate(state, componentsToDelete.toArray(String[]::new));
        }

        return new TransformState(state, Sets.union(componentEntities, composableEntities));
    }

    @Override
    public ComponentsAndComposables fromXContent(XContentParser parser) throws IOException {
        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        Map<String, ComposableIndexTemplate> composableTemplates = new HashMap<>();
        Map<String, ?> source = parser.map();

        Map<String, ?> components = (Map<String, ?>) source.get(COMPONENTS);

        if (components != null) {
            for (var entry : components.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> content = (Map<String, ?>) entry.getValue();
                try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                    componentTemplates.put(entry.getKey(), ComponentTemplate.parse(componentParser));
                }
            }
        }

        Map<String, ?> composables = (Map<String, ?>)source.get(COMPOSABLES);

        if (components != null) {
            for (var entry : composables.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> content = (Map<String, ?>) entry.getValue();
                try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                    composableTemplates.put(entry.getKey(), ComposableIndexTemplate.parse(componentParser));
                }
            }
        }

        return new ComponentsAndComposables(componentTemplates, composableTemplates);
    }

    record ComponentsAndComposables(
        Map<String, ComponentTemplate> componentTemplates,
        Map<String, ComposableIndexTemplate> composableTemplates
    ) {}
}
