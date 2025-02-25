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
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MetadataContentService {

    private static final Logger logger = LogManager.getLogger(MetadataContentService.class);
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<PutContentPackClusterStateUpdateTask> taskQueue;
    private final IndicesService indicesService;
    private final MetadataIndexTemplateService metadataIndexTemplateService;
    private final NamedXContentRegistry xContentRegistry;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final IngestService ingestService;

    /**
     * This is the cluster state task executor for all bulk content installation actions.
     */
    private static final SimpleBatchedExecutor<PutContentPackClusterStateUpdateTask, Void> CONTENT_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(PutContentPackClusterStateUpdateTask task, ClusterState clusterState)
                throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(PutContentPackClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private class PutContentPackClusterStateUpdateTask implements ClusterStateTaskListener {
        final List<PutPipelineRequest> ingestPipelines;
        final List<ComponentTemplateOperation> componentTemplateOperations;
        final List<ComposableTemplateOperation> composableTemplateOperations;
        final ActionListener<AcknowledgedResponse> listener;

        PutContentPackClusterStateUpdateTask(
            List<PutPipelineRequest> ingestPipelines,
            List<ComponentTemplateOperation> componentTemplateOperations,
            List<ComposableTemplateOperation> composableTemplateOperations,
            ActionListener<AcknowledgedResponse> listener
        ) {
            this.ingestPipelines = ingestPipelines;
            this.componentTemplateOperations = componentTemplateOperations;
            this.composableTemplateOperations = composableTemplateOperations;
            this.listener = listener;
        }

        public ClusterState execute(ClusterState currentState) throws Exception {
            return processContentPackUpdate(currentState, ingestPipelines, componentTemplateOperations, composableTemplateOperations);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    @Inject
    public MetadataContentService(
        ClusterService clusterService,
        MetadataIndexTemplateService metadataIndexTemplateService,
        IndicesService indicesService,
        NamedXContentRegistry xContentRegistry,
        DataStreamGlobalRetentionSettings globalRetentionSettings,
        IngestService ingestService
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("plugin-contents", Priority.URGENT, CONTENT_TASK_EXECUTOR);
        this.indicesService = indicesService;
        this.metadataIndexTemplateService = metadataIndexTemplateService;
        this.xContentRegistry = xContentRegistry;
        this.globalRetentionSettings = globalRetentionSettings;
        this.ingestService = ingestService;
    }

    public record IngestPipelineOperation(String id, BytesReference content, XContentType xContentType) {}

    /**
     * A simple request holder for component template updates
     * @param name
     * @param template
     * @param create
     */
    public record ComponentTemplateOperation(String name, ComponentTemplate template, boolean create) {}

    /**
     * A simple request holder for index template updates
     * @param name
     * @param template
     * @param create
     */
    public record ComposableTemplateOperation(String name, ComposableIndexTemplate template, boolean create) {}

    /**
     * A name/template tuple record
     * @param name of template
     * @param template definition
     */
    public record NamedTemplateTuple(String name, ComposableIndexTemplate template) {}

    /**
     * The collection of contents contained in this content pack that will be bulk updated/installed
     * @param ingestPipelines
     * @param componentTemplateOperations
     * @param composableTemplateOperations
     * @param masterTimeout
     */
    public record ContentPack(
        List<IngestPipelineOperation> ingestPipelines,
        List<ComponentTemplateOperation> componentTemplateOperations,
        List<ComposableTemplateOperation> composableTemplateOperations,
        TimeValue masterTimeout
    ) {}

    /**
     * Bulk install/update all provided pipelines, templates, and lifecycles into the cluster state
     * @param cause information about caller to identify this operation in the pending tasks
     * @param contentPack collection of content needed to be installed/updated in this operation
     * @param listener called upon completion of the installation
     * @param nodeInfoListener optionally called to collect NodesInfoResponse for pipeline validation if applicable
     */
    public void installContentPack(
        final String cause,
        final ContentPack contentPack,
        final ActionListener<AcknowledgedResponse> listener,
        Consumer<ActionListener<NodesInfoResponse>> nodeInfoListener
    ) {
        // PRTODO: Is this cluster state by-definition less fresh than the one we maintain in the IngestService?
        //  Is this safe to check against?
        ClusterState clusterState = clusterService.state();
        List<PutPipelineRequest> ingestPipelines = new ArrayList<>();
        for (IngestPipelineOperation ingestPipeline : contentPack.ingestPipelines) {
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(
                contentPack.masterTimeout,
                contentPack.masterTimeout,
                ingestPipeline.id,
                ingestPipeline.content,
                ingestPipeline.xContentType
            );
            if (IngestService.isNoOpPipelineUpdate(clusterState, putPipelineRequest)) {
                // existing pipeline matches request pipeline -- no need to update
                continue;
            }
            ingestPipelines.add(putPipelineRequest);
        }

        if (ingestPipelines.isEmpty()) {
            // Don't need nodes info to validate pipelines, move along
            taskQueue.submitTask(
                "create-content-pack, cause [" + cause + "]",
                new PutContentPackClusterStateUpdateTask(
                    List.of(),
                    contentPack.componentTemplateOperations,
                    contentPack.composableTemplateOperations,
                    listener
                ),
                contentPack.masterTimeout
            );
        } else {
            nodeInfoListener.accept(listener.delegateFailureAndWrap((l, nodesInfoResponse) -> {
                // Verify each pipeline can be installed on each node before submitting the cluster state update
                for (PutPipelineRequest ingestPipeline : ingestPipelines) {
                    ingestService.validatePipelineRequest(ingestPipeline, nodesInfoResponse);
                }

                taskQueue.submitTask(
                    "create-content-pack, cause [" + cause + "]",
                    new PutContentPackClusterStateUpdateTask(
                        ingestPipelines,
                        contentPack.componentTemplateOperations,
                        contentPack.composableTemplateOperations,
                        listener
                    ),
                    contentPack.masterTimeout
                );
            }));
        }
    }

    ClusterState processContentPackUpdate(
        final ClusterState currentState,
        List<PutPipelineRequest> ingestPipelines,
        List<ComponentTemplateOperation> componentTemplateOperations,
        List<ComposableTemplateOperation> composableTemplateOperations
    ) throws Exception {
        // Process pipelines into cluster state
        final IngestMetadata initialIngestMetadata = currentState.metadata().custom(IngestMetadata.TYPE);
        final IngestMetadata finalIngestMetadata = IngestService.clusterStateBulkUpdatePipelines(initialIngestMetadata, ingestPipelines);
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        if (finalIngestMetadata == initialIngestMetadata) {
            metadataBuilder.putCustom(IngestMetadata.TYPE, finalIngestMetadata);
        }

        // Process templates into cluster state - updates metadata builder by side effect
        InterimTemplateValidationInfo templateInfo = processBulkTemplateUpdate(
            currentState,
            componentTemplateOperations,
            composableTemplateOperations,
            currentState.metadata().componentTemplates(),
            currentState.metadata().templatesV2(),
            metadataBuilder
        );

        // Build cluster state with all updates from the parent bulk content pack operation
        ClusterState candidateState = ClusterState.builder(currentState).metadata(metadataBuilder).build();

        validateFinalClusterState(candidateState, templateInfo);

        return candidateState;
    }

    /**
     * Holds interim information from the template installation process about what needs validating still.
     * @param allTemplatesRequiringValidation
     * @param updatedComponentTemplates
     * @param updatedComposableIndexTemplates
     */
    private record InterimTemplateValidationInfo(
        Map<String, ComposableIndexTemplate> allTemplatesRequiringValidation,
        Map<String, ComponentTemplate> updatedComponentTemplates,
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates
    ) {}

    InterimTemplateValidationInfo processBulkTemplateUpdate(
        final ClusterState unupdatedPreviousClusterState,
        final List<ComponentTemplateOperation> componentTemplateOperations,
        final List<ComposableTemplateOperation> composableTemplateOperations,
        final Map<String, ComponentTemplate> intialComponentTemplates,
        final Map<String, ComposableIndexTemplate> initialTemplatesV2,
        final Metadata.Builder metadataBuilder
    ) throws Exception {
        Map<String, ComponentTemplate> updatedComponentTemplates = new HashMap<>();
        Map<String, ComposableIndexTemplate> updatedComposableIndexTemplates = new HashMap<>();

        // Keep a union of current and updated composable index templates to represent the updated cluster state contents
        Map<String, ComponentTemplate> workingComponents = new HashMap<>(intialComponentTemplates);
        Map<String, ComposableIndexTemplate> workingTemplatesV2 = new HashMap<>(initialTemplatesV2);

        // Process and finalize all templates to be operated on
        for (ComponentTemplateOperation componentTemplate : componentTemplateOperations) {
            final ComponentTemplate maybeFinalComponentTemplate = prepareComponentTemplate(
                intialComponentTemplates,
                componentTemplate.create,
                componentTemplate.name,
                componentTemplate.template
            );
            if (maybeFinalComponentTemplate != null) {
                updatedComponentTemplates.put(componentTemplate.name, maybeFinalComponentTemplate);
                workingComponents.put(componentTemplate.name, maybeFinalComponentTemplate);
            }
        }

        for (ComposableTemplateOperation composableIndexTemplate : composableTemplateOperations) {
            final ComposableIndexTemplate maybeFinalComposableTemplate = prepareComposableTemplate(
                unupdatedPreviousClusterState,
                initialTemplatesV2,
                composableIndexTemplate.create,
                composableIndexTemplate.name,
                composableIndexTemplate.template
            );
            if (maybeFinalComposableTemplate != null) {
                updatedComposableIndexTemplates.put(composableIndexTemplate.name, maybeFinalComposableTemplate);
                workingTemplatesV2.put(composableIndexTemplate.name, maybeFinalComposableTemplate);
            }
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
        for (Map.Entry<String, ComponentTemplate> component : updatedComponentTemplates.entrySet()) {
            metadataBuilder.put(component.getKey(), component.getValue());
        }
        for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : updatedComposableIndexTemplates.entrySet()) {
            metadataBuilder.put(indexTemplate.getKey(), indexTemplate.getValue());
        }

        // We need to build the final cluster state before we do any more validation on the templates, but building it here would force
        // this method to be the last in the chain of change applicators to the state. Instead, stash handles to these validation bits
        // to use once the entire cluster state is done being constructed and the candidate state can be validated.
        return new InterimTemplateValidationInfo(
            allTemplatesRequiringValidation,
            updatedComponentTemplates,
            updatedComposableIndexTemplates
        );
    }

    private void validateFinalClusterState(ClusterState candidateState, InterimTemplateValidationInfo templateInfo) throws Exception {
        // Validate all changed composable index templates that have been updated, either directly or by changes to their dependencies
        if (templateInfo.allTemplatesRequiringValidation.isEmpty() == false) {
            Exception validationFailure = null;
            for (Map.Entry<String, ComposableIndexTemplate> entry : templateInfo.allTemplatesRequiringValidation.entrySet()) {
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
                                    templateInfo.updatedComponentTemplates,
                                    templateInfo.updatedComposableIndexTemplates
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

    private ComponentTemplate prepareComponentTemplate(
        Map<String, ComponentTemplate> intialComponentTemplates,
        boolean create,
        String name,
        ComponentTemplate template
    ) throws Exception {
        final ComponentTemplate existing = intialComponentTemplates.get(name);
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

        if (finalComponentTemplate.equals(intialComponentTemplates.get(name))) {
            return null;
        }

        // Immediate validation of the component template only
        MetadataIndexTemplateService.validateTemplate(finalSettings, wrappedMappings, indicesService);
        metadataIndexTemplateService.validate(name, finalComponentTemplate);
        if (finalComponentTemplate.template().lifecycle() != null) {
            // We do not know if this lifecycle will belong to an internal data stream, so we fall back to a non internal.
            finalComponentTemplate.template().lifecycle().addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(), false);
        }
        return finalComponentTemplate;
    }

    private ComposableIndexTemplate prepareComposableTemplate(
        final ClusterState previousState,
        final Map<String, ComposableIndexTemplate> initialTemplatesV2,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template
    ) throws IOException {
        final ComposableIndexTemplate existing = initialTemplatesV2.get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("index template [" + name + "] already exists");
        }

        Map<String, List<String>> overlaps = MetadataIndexTemplateService.findConflictingV1Templates(
            previousState,
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
            return null;
        }
        return finalIndexTemplate;
    }
}
