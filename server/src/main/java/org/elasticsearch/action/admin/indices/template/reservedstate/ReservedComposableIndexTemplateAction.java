/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.reservedstate;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

/**
 * This {@link ReservedClusterStateHandler} is responsible for reserved state
 * CRUD operations on composable index templates and component templates, e.g. file based settings.
 * <p>
 * Internally it uses {@link MetadataIndexTemplateService} to add, update and delete both composable
 * index templates and component templates. The reserved state handler is implemented as a joint handler
 * for both component templates and composable index templates, because of the inherent interdependencies
 * of the two separate types. For example, one cannot create composable index templates without first the
 * component definitions being in the cluster state, however, the opposite is true when deleting. This
 * circular dependency makes it impossible for separation of the two handlers.
 */
public class ReservedComposableIndexTemplateAction
    implements
        ReservedClusterStateHandler<ProjectMetadata, ReservedComposableIndexTemplateAction.ComponentsAndComposables> {
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

    // Since we can't split the reserved state handler into two separate handlers, because of the
    // circular dependency on create and delete, we must store both the component template keys and
    // the composable index template keys in the same reserved state handler. To be able to correctly
    // distinguish between the component names and the composable names, we prefix the reserved keys
    // when they are stored in the cluster state. Similarly, we remove the prefix when we need to perform
    // the REST API validation in the corresponding transport actions.

    /**
     * Prefixes the component template name with a prefix for storage in the cluster state
     * @param name component template name
     * @return prefixed component template name for storage in the reserved cluster state
     */
    public static String reservedComponentName(String name) {
        return COMPONENT_PREFIX + name;
    }

    /**
     * Removes the reserved cluster state prefix from the component template name
     * <p>
     * Once the prefix is removed we can use the name for conflict validation in {@link TransportPutComponentTemplateAction} and
     * {@link org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction}
     * @param name the prefixed reserved component template name
     * @return the un-prefixed component template name used for conflict validation at REST
     */
    public static String componentNameFromReservedName(String name) {
        assert name.startsWith(COMPONENT_PREFIX);
        return name.substring(COMPONENT_PREFIX.length());
    }

    /**
     * Prefixes the composable index template name with a prefix for storage in the cluster state
     * @param name composable index template name
     * @return prefixed composable index template name for storage in the reserved cluster state
     */
    public static String reservedComposableIndexName(String name) {
        return COMPOSABLE_PREFIX + name;
    }

    /**
     * Removes the reserved cluster state prefix from the composable index template name
     * <p>
     * Once the prefix is removed we can use the name for conflict validation in
     * {@link org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction} and
     * {@link org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction}
     * @param name the prefixed reserved composable index template name
     * @return the un-prefixed composable index template name used for conflict validation at REST
     */
    public static String composableIndexNameFromReservedName(String name) {
        assert name.startsWith(COMPOSABLE_PREFIX);
        return name.substring(COMPOSABLE_PREFIX.length());
    }

    private ComponentsAndComposables prepare(ComponentsAndComposables componentsAndComposables) {
        for (var request : componentsAndComposables.componentTemplates) {
            validate(request);
        }

        for (var request : componentsAndComposables.composableTemplates) {
            validate(request);
        }

        return componentsAndComposables;
    }

    @Override
    public TransformState<ProjectMetadata> transform(ComponentsAndComposables source, TransformState<ProjectMetadata> prevState)
        throws Exception {
        var requests = prepare(source);
        ProjectMetadata project = prevState.state();

        // We transform in the following order:
        // 1. create or update component templates (composable templates depend on them)
        // 2. create or update composable index templates (with disabled v2 overlap validation, we might delete some at step 3,
        // while, 2 and 3 cannot be reversed because of data streams)
        // 3. delete composable index templates (this will fail on attached data streams, unless we added higher priority one)
        // 4. validate for v2 composable template overlaps
        // 5. delete component templates (this will check if there are any related composable index templates and fail)

        var components = requests.componentTemplates;
        var composables = requests.composableTemplates;

        // 1. create or update component templates (composable templates depend on them)
        for (var request : components) {
            ComponentTemplate template = TransportPutComponentTemplateAction.normalizeComponentTemplate(
                request.componentTemplate(),
                indexScopedSettings
            );

            project = indexTemplateService.addComponentTemplate(project, false, request.name(), template);
        }

        // 2. create or update composable index templates, no overlap validation
        for (var request : composables) {
            MetadataIndexTemplateService.validateV2TemplateRequest(project, request.name(), request.indexTemplate());
            project = indexTemplateService.addIndexTemplateV2(project, false, request.name(), request.indexTemplate(), false);
        }

        Set<String> composableEntities = composables.stream().map(r -> reservedComposableIndexName(r.name())).collect(Collectors.toSet());
        Set<String> composablesToDelete = prevState.keys()
            .stream()
            .filter(k -> k.startsWith(COMPOSABLE_PREFIX) && composableEntities.contains(k) == false)
            .collect(Collectors.toSet());

        // 3. delete composable index templates (this will fail on attached data streams, unless we added a higher priority one)
        if (composablesToDelete.isEmpty() == false) {
            var composableNames = composablesToDelete.stream().map(c -> composableIndexNameFromReservedName(c)).toArray(String[]::new);
            project = MetadataIndexTemplateService.innerRemoveIndexTemplateV2(project, composableNames);
        }

        // 4. validate for v2 composable template overlaps
        for (var request : composables) {
            MetadataIndexTemplateService.v2TemplateOverlaps(project, request.name(), request.indexTemplate(), true);
        }

        Set<String> componentEntities = components.stream().map(r -> reservedComponentName(r.name())).collect(Collectors.toSet());
        Set<String> componentsToDelete = prevState.keys().stream().filter(k -> k.startsWith(COMPONENT_PREFIX)).collect(Collectors.toSet());
        componentsToDelete.removeAll(componentEntities);

        // 5. delete component templates (this will check if there are any related composable index templates and fail)
        if (componentsToDelete.isEmpty() == false) {
            var componentNames = componentsToDelete.stream().map(c -> componentNameFromReservedName(c)).toArray(String[]::new);
            project = MetadataIndexTemplateService.innerRemoveComponentTemplate(project, componentNames);
        }

        return new TransformState<>(project, Sets.union(componentEntities, composableEntities));
    }

    @Override
    public ComponentsAndComposables fromXContent(XContentParser parser) throws IOException {
        List<PutComponentTemplateAction.Request> componentTemplates = new ArrayList<>();
        List<TransportPutComposableIndexTemplateAction.Request> composableTemplates = new ArrayList<>();
        Map<String, ?> source = parser.map();

        @SuppressWarnings("unchecked")
        Map<String, ?> components = (Map<String, ?>) source.get(COMPONENTS);

        if (components != null) {
            for (var entry : components.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> content = (Map<String, ?>) entry.getValue();
                try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                    var componentTemplate = new PutComponentTemplateAction.Request(entry.getKey());
                    componentTemplate.componentTemplate(ComponentTemplate.parse(componentParser));
                    componentTemplates.add(componentTemplate);
                }
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, ?> composables = (Map<String, ?>) source.get(COMPOSABLES);

        if (composables != null) {
            for (var entry : composables.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, ?> content = (Map<String, ?>) entry.getValue();
                try (XContentParser componentParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                    var composableTemplate = new TransportPutComposableIndexTemplateAction.Request(entry.getKey());
                    composableTemplate.indexTemplate(ComposableIndexTemplate.parse(componentParser));
                    composableTemplates.add(composableTemplate);
                }
            }
        }

        return new ComponentsAndComposables(componentTemplates, composableTemplates);
    }

    record ComponentsAndComposables(
        List<PutComponentTemplateAction.Request> componentTemplates,
        List<TransportPutComposableIndexTemplateAction.Request> composableTemplates
    ) {}
}
