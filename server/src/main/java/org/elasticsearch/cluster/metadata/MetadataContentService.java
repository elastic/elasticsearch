/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MetadataContentService {

    private static final Logger logger = LogManager.getLogger(MetadataContentService.class);
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<ContentClusterStateUpdateTask> taskQueue;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final MetadataIndexTemplateService metadataIndexTemplateService;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;
    private final SystemIndices systemIndices;
    private final Set<IndexSettingProvider> indexSettingProviders;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    /**
     * This is the cluster state task executor for all bulk content installation actions.
     */
    private static final SimpleBatchedExecutor<ContentClusterStateUpdateTask, Void> CONTENT_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(ContentClusterStateUpdateTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(ContentClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private class ContentClusterStateUpdateTask implements ClusterStateTaskListener {
        final List<ComponentTemplateOperation> componentTemplateOperations;
        final List<ComposableTemplateOperation> composableTemplateOperations;
        final ActionListener<AcknowledgedResponse> listener;

        ContentClusterStateUpdateTask(
            List<ComponentTemplateOperation> componentTemplateOperations,
            List<ComposableTemplateOperation> composableTemplateOperations,
            ActionListener<AcknowledgedResponse> listener
        ) {
            this.componentTemplateOperations = componentTemplateOperations;
            this.composableTemplateOperations = composableTemplateOperations;
            this.listener = listener;
        }

        public ClusterState execute(ClusterState currentState) throws Exception {
            return processBulkTemplateUpdate(currentState, componentTemplateOperations, composableTemplateOperations);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    @Inject
    public MetadataContentService(
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService,
        MetadataIndexTemplateService metadataIndexTemplateService,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        NamedXContentRegistry xContentRegistry,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("plugin-contents", Priority.URGENT, CONTENT_TASK_EXECUTOR);
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.metadataIndexTemplateService = metadataIndexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
        this.systemIndices = systemIndices;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
        this.globalRetentionSettings = globalRetentionSettings;
    }

    public record ComponentTemplateOperation(String name, ComponentTemplate template, boolean create) {}
    public record ComposableTemplateOperation(String name, ComposableIndexTemplate template, boolean create) {}

    /**
     * A name/template tuple record
     * @param name of template
     * @param template definition
     */
    public record NamedTemplateTuple(String name, ComposableIndexTemplate template) {}

    public enum TemplateType {
        COMPONENT_TEMPLATE,
        COMPOSABLE_TEMPLATE
    }

    public record TemplateResult(String name, TemplateType type, Exception failure) {
        public boolean hasFailure() {
            return failure != null;
        }
    }

    /**
     * Add the given templates to the cluster state.
     */
    public void bulkPutTemplates(
        final String cause,
        final List<ComponentTemplateOperation> componentTemplateOperations,
        final List<ComposableTemplateOperation> composableTemplateOperations,
        final TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "bulk-create-templates, cause [" + cause + "]",
            new ContentClusterStateUpdateTask(componentTemplateOperations, composableTemplateOperations, listener),
            masterTimeout
        );
    }

    ClusterState processBulkTemplateUpdate(
        final ClusterState currentState,
        final List<ComponentTemplateOperation> componentTemplateOperations,
        final List<ComposableTemplateOperation> composableTemplateOperations
    ) throws Exception {
        Map<String, ComponentTemplate> updatedComponentTemplates = new HashMap<>();
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates = new HashMap<>();

        // PRTODO: Would it be easier to just update the cluster state and then use it, and discard the new state if validation fails?
        // Keep a union of current and updated composable index templates to represent the updated cluster state contents
        Map<String, ComponentTemplate> workingComponents = new HashMap<>(currentState.metadata().componentTemplates());
        Map<String, ComposableIndexTemplate> workingTemplatesV2 = new HashMap<>(currentState.metadata().templatesV2());

        // Process and finalize all templates to be operated on
        for (ComponentTemplateOperation componentTemplate : componentTemplateOperations) {
            final Optional<ComponentTemplate> maybeFinalComponentTemplate = prepareComponentTemplate(
                currentState,
                componentTemplate.create,
                componentTemplate.name,
                componentTemplate.template
            );
            maybeFinalComponentTemplate.ifPresent(
                finalComponentTemplate -> {
                    updatedComponentTemplates.put(componentTemplate.name, finalComponentTemplate);
                    workingComponents.put(componentTemplate.name, finalComponentTemplate);
                }
            );
        }

        for (ComposableTemplateOperation composableIndexTemplate : composableTemplateOperations) {
            final Optional<ComposableIndexTemplate> maybeFinalComposableTemplate = prepareComposableTemplate(
                currentState,
                composableIndexTemplate.create,
                composableIndexTemplate.name,
                composableIndexTemplate.template
            );
            maybeFinalComposableTemplate.ifPresent(
                finalComposableTemplate -> {
                    updatedComposableIndexTemplates.put(composableIndexTemplate.name, finalComposableTemplate);
                    workingTemplatesV2.put(composableIndexTemplate.name, finalComposableTemplate);
                }
            );
        }

        // Collect all the composable index templates that use any of the updated component template. Additionally, collect all updated
        // component templates that were used by any index template. We'll use both of these for validating that both sets of templates
        // are (still) valid after updating.
        final Map<String, ComposableIndexTemplate> allTemplatesRequiringValidation = new HashMap<>();
        final Map<String, List<NamedTemplateTuple>> updatedComponentsUsedByTemplates = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> workingTemplateV2 : workingTemplatesV2.entrySet()) {
            String indexTemplateName = workingTemplateV2.getKey();
            ComposableIndexTemplate indexTemplateDefinition = workingTemplateV2.getValue();
            boolean usesUpdatedComponents = false;

            // Check each component dependency to see if it was updated during this bulk operation
            for (String component : indexTemplateDefinition.composedOf()) {
                if (updatedComponentTemplates.containsKey(component)) {
                    usesUpdatedComponents = true;
                    // Capture this updated component's relationship with this index template
                    updatedComponentsUsedByTemplates.compute(component, (key, value) -> {
                        List<NamedTemplateTuple> values = value == null ? new ArrayList<>() : value;
                        values.add(new NamedTemplateTuple(indexTemplateName, indexTemplateDefinition));
                        return values;
                    });
                }
            }

            // Capture all index templates that need to validate their composition
            if (usesUpdatedComponents || updatedComposableIndexTemplates.containsKey(indexTemplateName)) {
                allTemplatesRequiringValidation.put(indexTemplateName, indexTemplateDefinition);
            }
        }

        // Validate all the templates to make sure they aren't adding hidden index settings to global patterns
        for (Map.Entry<String, ComposableIndexTemplate> updatedComposableIndexTemplate : updatedComposableIndexTemplates.entrySet()) {
            String indexTemplateName = updatedComposableIndexTemplate.getKey();
            ComposableIndexTemplate composableIndexTemplate = updatedComposableIndexTemplate.getValue();

            MetadataIndexTemplateService.validateV2TemplateRequest(workingComponents, indexTemplateName, composableIndexTemplate);
            MetadataIndexTemplateService.v2TemplateOverlaps(workingTemplatesV2, indexTemplateName, composableIndexTemplate, true);
        }
        // Do the same validation for the component template changes
        // PRTODO: Is this redundant with the validateV2TemplateRequest call in the block right before here?
        for (Map.Entry<String, List<NamedTemplateTuple>> updatedComponentUsedByTemplate : updatedComponentsUsedByTemplates.entrySet()) {
            String componentTemplateName = updatedComponentUsedByTemplate.getKey();
            List<NamedTemplateTuple> composableTemplatesUsingThisComponent = updatedComponentUsedByTemplate.getValue();

            ComponentTemplate componentTemplate = updatedComponentTemplates.get(componentTemplateName);
            validateNoHiddenSettingOnGlobalTemplates(componentTemplateName, componentTemplate, composableTemplatesUsingThisComponent);
        }

        // Sufficiently validated enough to apply changes to a candidate cluster state to complete the rest of the validation
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        for (Map.Entry<String, ComponentTemplate> component : updatedComponentTemplates.entrySet()) {
            metadataBuilder.put(component.getKey(), component.getValue());
        }
        for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : updatedComposableIndexTemplates.entrySet()) {
            metadataBuilder.put(indexTemplate.getKey(), indexTemplate.getValue());
        }
        ClusterState candidateState = ClusterState.builder(currentState).metadata(metadataBuilder).build();

        // Validate all changed composable index templates that have been updated, either directly or by changes to their dependencies
        if (allTemplatesRequiringValidation.isEmpty() == false) {
            Exception validationFailure = null;
            for (Map.Entry<String, ComposableIndexTemplate> entry : allTemplatesRequiringValidation.entrySet()) {
                final String composableTemplateName = entry.getKey();
                final ComposableIndexTemplate composableTemplate = entry.getValue();
                try {
                    metadataIndexTemplateService.validateIndexTemplateV2(
                        composableTemplateName,
                        composableTemplate,
                        candidateState
                    );
                } catch (Exception e) {
                    if (validationFailure == null) {
                        validationFailure = new IllegalArgumentException(
                            "bulk updating templates ["
                                + generateTemplateNamesForException(
                                    composableTemplateName,
                                    composableTemplate,
                                    updatedComponentTemplates,
                                    updatedComposableIndexTemplates
                                )
                                + "] results in invalid composable template ["
                                + composableTemplateName
                                + "] after templates are merged",
                            e
                        );
                    } else {
                        validationFailure.addSuppressed(e);
                    }
                }
            }
            if (validationFailure != null) {
                throw validationFailure;
            }
        }

        return candidateState;
    }

    private static StringBuilder generateTemplateNamesForException(
        String composableTemplateName,
        ComposableIndexTemplate composableTemplate,
        Map<String, ComponentTemplate> updatedComponentTemplates,
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates
    ) {
        StringBuilder builder = new StringBuilder();
        var failingComponents = composableTemplate.composedOf()
            .stream()
            .filter(updatedComponentTemplates::containsKey)
            .collect(Collectors.joining(", "));
        if (failingComponents.isEmpty() == false) {
            builder.append(failingComponents);
        }
        var failingTemplate = updatedComposableIndexTemplates.containsKey(composableTemplateName)
            ? composableTemplateName
            : null;
        if (failingTemplate != null) {
            if (failingComponents.isEmpty() == false) {
                builder.append(", ");
            }
            builder.append(failingTemplate);
        }
        return builder;
    }

    /**
     * Validate that a component template does not set an index to hidden if it is used within a global template (match *)
     * @param name of the component template to check
     * @param componentTemplate definition of the template to check
     * @param templatesUsingComponent all index templates that currently make use of this component
     */
    private static void validateNoHiddenSettingOnGlobalTemplates(
        String name,
        ComponentTemplate componentTemplate,
        List<NamedTemplateTuple> templatesUsingComponent
    ) {

        Settings finalSettings = Optional.of(componentTemplate).map(ComponentTemplate::template).map(Template::settings).orElse(null);
        if (finalSettings != null) {
            // if the CT is specifying the `index.hidden` setting it cannot be part of any global template
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(finalSettings)) {
                List<String> globalTemplatesThatUseThisComponent = new ArrayList<>();
                for (NamedTemplateTuple template : templatesUsingComponent) {
                    ComposableIndexTemplate templateV2 = template.template;
                    if (templateV2.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                        // global templates don't support configuring the `index.hidden` setting so we don't need to resolve the settings as
                        // no other component template can remove this setting from the resolved settings, so just invalidate this update
                        globalTemplatesThatUseThisComponent.add(template.name);
                    }
                }
                if (globalTemplatesThatUseThisComponent.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        "cannot update component template ["
                            + name
                            + "] because the following global templates would resolve to specifying the ["
                            + IndexMetadata.SETTING_INDEX_HIDDEN
                            + "] setting: ["
                            + String.join(",", globalTemplatesThatUseThisComponent)
                            + "]"
                    );
                }
            }
        }
    }

    private Optional<ComponentTemplate> prepareComponentTemplate(
        ClusterState currentState,
        boolean create,
        String name,
        ComponentTemplate template
    ) throws Exception {
        final ComponentTemplate existing = currentState.metadata().componentTemplates().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("component template [" + name + "] already exists");
        }

        CompressedXContent mappings = template.template().mappings();
        CompressedXContent wrappedMappings = MetadataIndexTemplateService.wrapMappingsIfNecessary(mappings, xContentRegistry);

        // We may need to normalize index settings, so do that also
        Settings finalSettings = template.template().settings();
        if (finalSettings != null) {
            finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        }

        final Template finalTemplate = Template.builder(template.template()).settings(finalSettings).mappings(wrappedMappings).build();
        final ComponentTemplate finalComponentTemplate = new ComponentTemplate(
            finalTemplate,
            template.version(),
            template.metadata(),
            template.deprecated()
        );

        if (finalComponentTemplate.equals(currentState.metadata().componentTemplates().get(name))) {
            return Optional.empty();
        }

        // Immediate validation of the component template only
        MetadataIndexTemplateService.validateTemplate(finalSettings, wrappedMappings, indicesService);
        metadataIndexTemplateService.validate(name, finalComponentTemplate);
        if (finalComponentTemplate.template().lifecycle() != null) {
            // We do not know if this lifecycle will belong to an internal data stream, so we fall back to a non internal.
            finalComponentTemplate.template().lifecycle().addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(), false);
        }
        return Optional.of(finalComponentTemplate);
    }

    private Optional<ComposableIndexTemplate> prepareComposableTemplate(
        final ClusterState currentState,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template
    ) throws IOException {
        final ComposableIndexTemplate existing = currentState.metadata().templatesV2().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("index template [" + name + "] already exists");
        }

        Map<String, List<String>> overlaps = MetadataIndexTemplateService.findConflictingV1Templates(
            currentState,
            name,
            template.indexPatterns()
        );
        if (overlaps.size() > 0) {
            String warning = String.format(
                Locale.ROOT,
                "index template [%s] has index patterns %s matching patterns from "
                    + "existing older templates [%s] with patterns (%s); this template [%s] will take precedence during new index creation",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                name
            );
            logger.warn(warning);
            HeaderWarning.addWarning(warning);
        }

        // Normalize the internal template part of this component template if it is present
        ComposableIndexTemplate finalIndexTemplate = template;
        Template innerTemplate = template.template();
        if (innerTemplate != null) {
            // We may need to normalize index settings, so do that also
            Settings finalSettings = innerTemplate.settings();
            if (finalSettings != null) {
                finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
            }
            // If an inner template was specified, its mappings may need to be
            // adjusted (to add _doc) and it should be validated
            CompressedXContent mappings = innerTemplate.mappings();
            CompressedXContent wrappedMappings = MetadataIndexTemplateService.wrapMappingsIfNecessary(mappings, xContentRegistry);
            final Template finalTemplate = Template.builder(innerTemplate).settings(finalSettings).mappings(wrappedMappings).build();
            finalIndexTemplate = template.toBuilder().template(finalTemplate).build();
        }

        // If this finalized template hasn't changed compared to what is in the cluster, skip updating it
        if (finalIndexTemplate.equals(existing)) {
            return Optional.empty();
        }
        return Optional.of(finalIndexTemplate);
    }
}
