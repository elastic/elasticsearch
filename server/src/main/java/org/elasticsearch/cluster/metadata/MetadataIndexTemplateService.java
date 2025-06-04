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
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.CloseUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.validateTimestampFieldMapping;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting index templates updates
 */
public class MetadataIndexTemplateService {

    public static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    public static final CompressedXContent DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING;
    // Names used for validating templates when we do not know the index or data stream name
    public static final String VALIDATE_INDEX_NAME = "validate-index-name";
    public static final String VALIDATE_DATA_STREAM_NAME = "validate-data-stream-name";

    private static final CompressedXContent DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING;

    static {
        final Map<String, Map<String, String>> defaultTimestampField = Map.of(
            DEFAULT_TIMESTAMP_FIELD,
            // We inject ignore_malformed false so that if a user does not add the timestamp field it will explicitly skip applying any
            // other ignore_malformed configurations from the index settings.
            Map.of("type", DateFieldMapper.CONTENT_TYPE, "ignore_malformed", "false")
        );
        try {
            DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    // adding explicit "_routing": {"required": false}, even though this is the default, because this snippet is used
                    // later for resolving a RoutingFieldMapper, where we need this information to validate that does not conflict with
                    // any mapping.
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", false)
                    .endObject()
                    .field("properties", defaultTimestampField)
                    .endObject()
            );
            DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", true)
                    .endObject()
                    .field("properties")
                    .map(defaultTimestampField)
                    .endObject()
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static final Logger logger = LogManager.getLogger(MetadataIndexTemplateService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MetadataIndexTemplateService.class);

    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<TemplateClusterStateUpdateTask> taskQueue;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;
    private final SystemIndices systemIndices;
    private final Set<IndexSettingProvider> indexSettingProviders;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    /**
     * This is the cluster state task executor for all template-based actions.
     */
    private static final SimpleBatchedExecutor<TemplateClusterStateUpdateTask, Void> TEMPLATE_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(TemplateClusterStateUpdateTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(TemplateClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private abstract static class TemplateClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;
        final ProjectId projectId;

        TemplateClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener, ProjectId projectId) {
            this.listener = listener;
            this.projectId = projectId;
        }

        public final ClusterState execute(ClusterState currentState) throws Exception {
            ProjectMetadata metadata = currentState.metadata().getProject(projectId);
            ProjectMetadata newMetadata = execute(metadata);
            return metadata == newMetadata ? currentState : ClusterState.builder(currentState).putProjectMetadata(newMetadata).build();
        }

        public abstract ProjectMetadata execute(ProjectMetadata currentProject) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    @Inject
    public MetadataIndexTemplateService(
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        NamedXContentRegistry xContentRegistry,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue("index-templates", Priority.URGENT, TEMPLATE_TASK_EXECUTOR);
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
        this.systemIndices = systemIndices;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
        this.globalRetentionSettings = globalRetentionSettings;
    }

    public void removeTemplates(
        final ProjectId projectId,
        final String templatePattern,
        final TimeValue timeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask("remove-index-template [" + templatePattern + "]", new TemplateClusterStateUpdateTask(listener, projectId) {
            @Override
            public ProjectMetadata execute(ProjectMetadata project) {
                Set<String> templateNames = new HashSet<>();
                for (Map.Entry<String, IndexTemplateMetadata> cursor : project.templates().entrySet()) {
                    String templateName = cursor.getKey();
                    if (Regex.simpleMatch(templatePattern, templateName)) {
                        templateNames.add(templateName);
                    }
                }
                if (templateNames.isEmpty()) {
                    // if its a match all pattern, and no templates are found (we have none), don't
                    // fail with index missing...
                    if (Regex.isMatchAllPattern(templatePattern)) {
                        return project;
                    }
                    throw new IndexTemplateMissingException(templatePattern);
                }
                ProjectMetadata.Builder metadata = ProjectMetadata.builder(project);
                for (String templateName : templateNames) {
                    logger.info("removing template [{}]", templateName);
                    metadata.removeTemplate(templateName);
                }
                return metadata.build();
            }
        }, timeout);
    }

    /**
     * Add the given component template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putComponentTemplate(
        final String cause,
        final boolean create,
        final String name,
        final TimeValue masterTimeout,
        final ComponentTemplate template,
        final ProjectId projectId,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "create-component-template [" + name + "], cause [" + cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) throws Exception {
                    return addComponentTemplate(currentProject, create, name, template);
                }
            },
            masterTimeout
        );
    }

    // Public visible for testing
    public ProjectMetadata addComponentTemplate(
        final ProjectMetadata project,
        final boolean create,
        final String name,
        final ComponentTemplate template
    ) throws Exception {
        final ComponentTemplate existing = project.componentTemplates().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("component template [" + name + "] already exists");
        }

        CompressedXContent mappings = template.template().mappings();
        CompressedXContent wrappedMappings = wrapMappingsIfNecessary(mappings, xContentRegistry);

        // We may need to normalize index settings, so do that also
        Settings finalSettings = template.template().settings();
        if (finalSettings != null) {
            finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        }

        // Collect all the composable (index) templates that use this component template, we'll use
        // this for validating that they're still going to be valid after this component template
        // has been updated
        final Map<String, ComposableIndexTemplate> templatesUsingComponent = project.templatesV2()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().composedOf().contains(name))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // if we're updating a component template, let's check if it's part of any V2 template that will yield the CT update invalid
        if (create == false && finalSettings != null) {
            // if the CT is specifying the `index.hidden` setting it cannot be part of any global template
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(finalSettings)) {
                List<String> globalTemplatesThatUseThisComponent = new ArrayList<>();
                for (Map.Entry<String, ComposableIndexTemplate> entry : templatesUsingComponent.entrySet()) {
                    ComposableIndexTemplate templateV2 = entry.getValue();
                    if (templateV2.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                        // global templates don't support configuring the `index.hidden` setting so we don't need to resolve the settings as
                        // no other component template can remove this setting from the resolved settings, so just invalidate this update
                        globalTemplatesThatUseThisComponent.add(entry.getKey());
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

        final Template finalTemplate = Template.builder(template.template()).settings(finalSettings).mappings(wrappedMappings).build();
        final ComponentTemplate finalComponentTemplate = new ComponentTemplate(
            finalTemplate,
            template.version(),
            template.metadata(),
            template.deprecated()
        );

        if (finalComponentTemplate.equals(existing)) {
            return project;
        }

        validateTemplate(finalSettings, wrappedMappings, indicesService);
        validate(name, finalComponentTemplate);

        // Validate all composable index templates that use this component template
        if (templatesUsingComponent.size() > 0) {
            ProjectMetadata tempProjectWithComponentTemplateAdded = ProjectMetadata.builder(project)
                .put(name, finalComponentTemplate)
                .build();
            Exception validationFailure = null;
            for (Map.Entry<String, ComposableIndexTemplate> entry : templatesUsingComponent.entrySet()) {
                final String composableTemplateName = entry.getKey();
                final ComposableIndexTemplate composableTemplate = entry.getValue();
                try {
                    validateLifecycle(
                        tempProjectWithComponentTemplateAdded,
                        composableTemplateName,
                        composableTemplate,
                        globalRetentionSettings.get(false)
                    );
                    validateDataStreamOptions(
                        tempProjectWithComponentTemplateAdded,
                        composableTemplateName,
                        composableTemplate,
                        globalRetentionSettings.get(true)
                    );
                    validateIndexTemplateV2(tempProjectWithComponentTemplateAdded, composableTemplateName, composableTemplate);
                } catch (Exception e) {
                    if (validationFailure == null) {
                        validationFailure = new IllegalArgumentException(
                            "updating component template ["
                                + name
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

        if (finalComponentTemplate.template().lifecycle() != null) {
            // We do not know if this lifecycle will belong to an internal data stream, so we fall back to a non internal.
            finalComponentTemplate.template()
                .lifecycle()
                .toDataStreamLifecycle()
                .addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(false), false);
        }

        logger.info("{} component template [{}]", existing == null ? "adding" : "updating", name);
        return ProjectMetadata.builder(project).put(name, finalComponentTemplate).build();
    }

    @Nullable
    private static CompressedXContent wrapMappingsIfNecessary(@Nullable CompressedXContent mappings, NamedXContentRegistry xContentRegistry)
        throws IOException {
        // Mappings in templates don't have to include _doc, so update
        // the mappings to include this single type if necessary

        CompressedXContent wrapped = mappings;
        if (wrapped != null) {
            Map<String, Object> parsedMappings = MapperService.parseMapping(xContentRegistry, mappings);
            if (parsedMappings.size() > 0) {
                if (parsedMappings.size() == 1) {
                    final String keyName = parsedMappings.keySet().iterator().next();
                    // Check if it's already wrapped in `_doc`, only rewrap if needed
                    if (MapperService.SINGLE_MAPPING_NAME.equals(keyName) == false) {
                        wrapped = new CompressedXContent(
                            (builder, params) -> builder.field(MapperService.SINGLE_MAPPING_NAME, parsedMappings)
                        );
                    }
                } else {
                    wrapped = new CompressedXContent((builder, params) -> builder.field(MapperService.SINGLE_MAPPING_NAME, parsedMappings));
                }
            }
        }
        return wrapped;
    }

    /**
     * Remove the given component template from the cluster state. The component template name
     * supports simple regex wildcards for removing multiple component templates at a time.
     */
    public void removeComponentTemplate(
        final String[] names,
        final TimeValue masterTimeout,
        final ProjectMetadata project,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        validateCanBeRemoved(project, names);
        taskQueue.submitTask(
            "remove-component-template [" + String.join(",", names) + "]",
            new TemplateClusterStateUpdateTask(listener, project.id()) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) {
                    return innerRemoveComponentTemplate(currentProject, names);
                }
            },
            masterTimeout
        );
    }

    // Exposed for ReservedComponentTemplateAction
    public static ProjectMetadata innerRemoveComponentTemplate(ProjectMetadata project, String... names) {
        validateCanBeRemoved(project, names);

        final Set<String> templateNames = new HashSet<>();
        if (names.length > 1) {
            Set<String> missingNames = null;
            for (String name : names) {
                if (project.componentTemplates().containsKey(name)) {
                    templateNames.add(name);
                } else {
                    // wildcards are not supported, so if a name with a wildcard is specified then
                    // the else clause gets executed, because template names can't contain a wildcard.
                    if (missingNames == null) {
                        missingNames = new LinkedHashSet<>();
                    }
                    missingNames.add(name);
                }
            }

            if (missingNames != null) {
                throw new ResourceNotFoundException(String.join(",", missingNames));
            }
        } else {
            for (String templateName : project.componentTemplates().keySet()) {
                if (Regex.simpleMatch(names[0], templateName)) {
                    templateNames.add(templateName);
                }
            }
            if (templateNames.isEmpty()) {
                // if its a match all pattern, and no templates are found (we have none), don't
                // fail with index missing...
                if (Regex.isMatchAllPattern(names[0])) {
                    return project;
                }
                throw new ResourceNotFoundException(names[0]);
            }
        }
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        for (String templateName : templateNames) {
            logger.info("removing component template [{}]", templateName);
            builder.removeComponentTemplate(templateName);
        }
        return builder.build();
    }

    /**
     * Validates that the given component template can be removed, throwing an error if it cannot.
     * A component template should not be removed if it is <b>required</b> by any index templates,
     * that is- it is used AND NOT specified as {@code ignore_missing_component_templates}.
     */
    static void validateCanBeRemoved(ProjectMetadata project, String... templateNameOrWildcard) {
        final Predicate<String> predicate;
        if (templateNameOrWildcard.length > 1) {
            predicate = name -> Arrays.asList(templateNameOrWildcard).contains(name);
        } else {
            predicate = name -> Regex.simpleMatch(templateNameOrWildcard[0], name);
        }
        final Set<String> matchingComponentTemplates = project.componentTemplates()
            .keySet()
            .stream()
            .filter(predicate)
            .collect(Collectors.toSet());
        final Set<String> componentsBeingUsed = new HashSet<>();
        final List<String> templatesStillUsing = project.templatesV2().entrySet().stream().filter(e -> {
            Set<String> intersecting = Sets.intersection(
                new HashSet<>(e.getValue().getRequiredComponentTemplates()),
                matchingComponentTemplates
            );
            if (intersecting.size() > 0) {
                componentsBeingUsed.addAll(intersecting);
                return true;
            }
            return false;
        }).map(Map.Entry::getKey).toList();

        if (templatesStillUsing.size() > 0) {
            throw new IllegalArgumentException(
                "component templates "
                    + componentsBeingUsed
                    + " cannot be removed as they are still in use by index templates "
                    + templatesStillUsing
            );
        }
    }

    /**
     * Add the given index template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putIndexTemplateV2(
        final String cause,
        final boolean create,
        final String name,
        final TimeValue masterTimeout,
        final ComposableIndexTemplate template,
        final ProjectId projectId,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        var project = clusterService.state().metadata().getProject(projectId);
        validateV2TemplateRequest(project, name, template);
        taskQueue.submitTask(
            "create-index-template-v2 [" + name + "], cause [" + cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) throws Exception {
                    return addIndexTemplateV2(currentProject, create, name, template);
                }
            },
            masterTimeout
        );
    }

    public static void validateV2TemplateRequest(ProjectMetadata metadata, String name, ComposableIndexTemplate template) {
        if (template.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
            Settings mergedSettings = resolveSettings(template, metadata.componentTemplates());
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(mergedSettings)) {
                throw new InvalidIndexTemplateException(
                    name,
                    "global composable templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey()
                );
            }
        }

        final Map<String, ComponentTemplate> componentTemplates = metadata.componentTemplates();
        final List<String> ignoreMissingComponentTemplates = (template.getIgnoreMissingComponentTemplates() == null
            ? List.of()
            : template.getIgnoreMissingComponentTemplates());
        final List<String> missingComponentTemplates = template.composedOf()
            .stream()
            .filter(componentTemplate -> componentTemplates.containsKey(componentTemplate) == false)
            .filter(componentTemplate -> ignoreMissingComponentTemplates.contains(componentTemplate) == false)
            .toList();

        if (missingComponentTemplates.size() > 0 && ignoreMissingComponentTemplates.size() == 0) {
            throw new InvalidIndexTemplateException(
                name,
                "index template [" + name + "] specifies component templates " + missingComponentTemplates + " that do not exist"
            );
        }

        if (missingComponentTemplates.size() > 0 && ignoreMissingComponentTemplates.size() > 0) {

            throw new InvalidIndexTemplateException(
                name,
                "index template ["
                    + name
                    + "] specifies a missing component templates "
                    + missingComponentTemplates
                    + " "
                    + "that does not exist and is not part of 'ignore_missing_component_templates'"
            );
        }
    }

    public ProjectMetadata addIndexTemplateV2(
        final ProjectMetadata project,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template
    ) throws Exception {
        return addIndexTemplateV2(project, create, name, template, true);
    }

    public ProjectMetadata addIndexTemplateV2(
        final ProjectMetadata project,
        final boolean create,
        final String name,
        final ComposableIndexTemplate template,
        final boolean validateV2Overlaps
    ) throws Exception {
        final ComposableIndexTemplate existing = project.templatesV2().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("index template [" + name + "] already exists");
        }

        Map<String, List<String>> overlaps = v2TemplateOverlaps(project, name, template, validateV2Overlaps);

        overlaps = findConflictingV1Templates(project, name, template.indexPatterns());
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

        final ComposableIndexTemplate finalIndexTemplate;
        Template innerTemplate = template.template();
        if (innerTemplate == null) {
            finalIndexTemplate = template;
        } else {
            // We may need to normalize index settings, so do that also
            Settings finalSettings = innerTemplate.settings();
            if (finalSettings != null) {
                finalSettings = Settings.builder().put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
            }
            // If an inner template was specified, its mappings may need to be
            // adjusted (to add _doc) and it should be validated
            CompressedXContent mappings = innerTemplate.mappings();
            CompressedXContent wrappedMappings = wrapMappingsIfNecessary(mappings, xContentRegistry);
            final Template finalTemplate = Template.builder(innerTemplate).settings(finalSettings).mappings(wrappedMappings).build();
            finalIndexTemplate = template.toBuilder().template(finalTemplate).build();
        }

        if (finalIndexTemplate.equals(existing)) {
            return project;
        }

        validateIndexTemplateV2(project, name, finalIndexTemplate);
        logger.info(
            "{} index template [{}] for index patterns {}",
            existing == null ? "adding" : "updating",
            name,
            template.indexPatterns()
        );
        return ProjectMetadata.builder(project).put(name, finalIndexTemplate).build();
    }

    /**
     * Calculates the conflicting v2 index template overlaps for a given composable index template. Optionally if validate is true
     * we throw an {@link IllegalArgumentException} with information about the conflicting templates.
     * <p>
     * This method doesn't check for conflicting overlaps with v1 templates.
     * @param project the current project
     * @param name the composable index template name
     * @param template the full composable index template object we check for overlaps
     * @param validate should we throw {@link IllegalArgumentException} if conflicts are found or just compute them
     * @return a map of v2 template names to their index patterns for v2 templates that would overlap with the given template
     */
    public static Map<String, List<String>> v2TemplateOverlaps(
        final ProjectMetadata project,
        String name,
        final ComposableIndexTemplate template,
        boolean validate
    ) {
        Map<String, List<String>> overlaps = findConflictingV2Templates(
            project,
            name,
            template.indexPatterns(),
            true,
            template.priorityOrZero()
        );
        overlaps.remove(name);
        if (validate && overlaps.size() > 0) {
            String error = String.format(
                Locale.ROOT,
                "index template [%s] has index patterns %s matching patterns from "
                    + "existing templates [%s] with patterns (%s) that have the same priority [%d], multiple index templates may not "
                    + "match during index creation, please use a different priority",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                template.priorityOrZero()
            );
            throw new IllegalArgumentException(error);
        }

        return overlaps;
    }

    // Visibility for testing
    void validateIndexTemplateV2(ProjectMetadata projectMetadata, String name, ComposableIndexTemplate indexTemplate) {
        // Workaround for the fact that start_time and end_time are injected by the MetadataCreateDataStreamService upon creation,
        // but when validating templates that create data streams the MetadataCreateDataStreamService isn't used.
        var finalTemplate = indexTemplate.template();
        final var now = Instant.now();

        final var combinedMappings = collectMappings(indexTemplate, projectMetadata.componentTemplates(), "tmp_idx");
        final var combinedSettings = resolveSettings(indexTemplate, projectMetadata.componentTemplates());
        // First apply settings sourced from index setting providers:
        var finalSettings = Settings.builder();
        for (var provider : indexSettingProviders) {
            var newAdditionalSettings = provider.getAdditionalIndexSettings(
                VALIDATE_INDEX_NAME,
                indexTemplate.getDataStreamTemplate() != null ? VALIDATE_DATA_STREAM_NAME : null,
                projectMetadata.retrieveIndexModeFromTemplate(indexTemplate),
                projectMetadata,
                now,
                combinedSettings,
                combinedMappings
            );
            MetadataCreateIndexService.validateAdditionalSettings(provider, newAdditionalSettings, finalSettings);
            finalSettings.put(newAdditionalSettings);
        }
        // Then apply setting from component templates:
        finalSettings.put(combinedSettings);
        // Then finally apply settings resolved from index template:
        if (finalTemplate != null && finalTemplate.settings() != null) {
            finalSettings.put(finalTemplate.settings());
        }

        var templateToValidate = indexTemplate.toBuilder().template(Template.builder(finalTemplate).settings(finalSettings)).build();

        validate(name, templateToValidate);
        validateDataStreamsStillReferenced(projectMetadata, name, templateToValidate);
        validateLifecycle(projectMetadata, name, templateToValidate, globalRetentionSettings.get(false));
        validateDataStreamOptions(projectMetadata, name, templateToValidate, globalRetentionSettings.get(true));

        if (templateToValidate.isDeprecated() == false) {
            validateUseOfDeprecatedComponentTemplates(name, templateToValidate, projectMetadata.componentTemplates());
            validateUseOfDeprecatedIngestPipelines(name, projectMetadata.custom(IngestMetadata.TYPE), combinedSettings);
            // TODO come up with a plan how to validate usage of deprecated ILM policies
            // we don't have access to the core/main plugin here so we can't use the IndexLifecycleMetadata type
        }

        // Finally, right before adding the template, we need to ensure that the composite settings,
        // mappings, and aliases are valid after it's been composed with the component templates
        try {
            validateCompositeTemplate(projectMetadata, name, templateToValidate, indicesService, xContentRegistry, systemIndices);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "composable template ["
                    + name
                    + "] template after composition "
                    + (indexTemplate.composedOf().size() > 0 ? "with component templates " + indexTemplate.composedOf() + " " : "")
                    + "is invalid",
                e
            );
        }
    }

    private void validateUseOfDeprecatedComponentTemplates(
        String name,
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        template.composedOf()
            .stream()
            .map(ct -> Tuple.tuple(ct, componentTemplates.get(ct)))
            .filter(ct -> Objects.nonNull(ct.v2()))
            .filter(ct -> ct.v2().isDeprecated())
            .forEach(
                ct -> deprecationLogger.warn(
                    DeprecationCategory.TEMPLATES,
                    "use_of_deprecated_component_template",
                    "index template [{}] uses deprecated component template [{}]",
                    name,
                    ct.v1()
                )
            );
    }

    private void validateUseOfDeprecatedIngestPipelines(String name, IngestMetadata ingestMetadata, Settings combinedSettings) {
        Map<String, PipelineConfiguration> pipelines = Optional.ofNullable(ingestMetadata)
            .map(IngestMetadata::getPipelines)
            .orElse(Map.of());
        emitWarningIfPipelineIsDeprecated(name, pipelines, combinedSettings.get("index.default_pipeline"));
        emitWarningIfPipelineIsDeprecated(name, pipelines, combinedSettings.get("index.final_pipeline"));
    }

    private void emitWarningIfPipelineIsDeprecated(String name, Map<String, PipelineConfiguration> pipelines, String pipelineName) {
        Optional.ofNullable(pipelineName)
            .map(pipelines::get)
            .filter(p -> Boolean.TRUE.equals(p.getConfig().get("deprecated")))
            .ifPresent(
                p -> deprecationLogger.warn(
                    DeprecationCategory.TEMPLATES,
                    "use_of_deprecated_ingest_pipeline",
                    "index template [{}] uses deprecated ingest pipeline [{}]",
                    name,
                    p.getId()
                )
            );
    }

    // Visible for testing
    static void validateLifecycle(
        ProjectMetadata project,
        String indexTemplateName,
        ComposableIndexTemplate template,
        @Nullable DataStreamGlobalRetention globalRetention
    ) {
        DataStreamLifecycle.Builder builder = resolveLifecycle(template, project.componentTemplates());
        if (builder != null) {
            if (template.getDataStreamTemplate() == null) {
                throw new IllegalArgumentException(
                    "index template ["
                        + indexTemplateName
                        + "] specifies lifecycle configuration that can only be used in combination with a data stream"
                );
            }
            if (globalRetention != null) {
                // We cannot know for sure if the template will apply to internal data streams, so we use a simpler heuristic:
                // If all the index patterns start with a dot, we consider that all the connected data streams are internal.
                boolean isInternalDataStream = template.indexPatterns().stream().allMatch(indexPattern -> indexPattern.charAt(0) == '.');
                builder.build().addWarningHeaderIfDataRetentionNotEffective(globalRetention, isInternalDataStream);
            }
        }
    }

    // Visible for testing
    static void validateDataStreamOptions(
        ProjectMetadata projectMetadata,
        String indexTemplateName,
        ComposableIndexTemplate template,
        DataStreamGlobalRetention globalRetention
    ) {
        DataStreamOptions.Builder dataStreamOptionsBuilder = resolveDataStreamOptions(template, projectMetadata.componentTemplates());
        if (dataStreamOptionsBuilder != null) {
            if (template.getDataStreamTemplate() == null) {
                throw new IllegalArgumentException(
                    "index template ["
                        + indexTemplateName
                        + "] specifies data stream options that can only be used in combination with a data stream"
                );
            }
            if (globalRetention != null) {
                // We cannot know for sure if the template will apply to internal data streams, so we use a simpler heuristic:
                // If all the index patterns start with a dot, we consider that all the connected data streams are internal.
                boolean isInternalDataStream = template.indexPatterns().stream().allMatch(indexPattern -> indexPattern.charAt(0) == '.');
                DataStreamOptions dataStreamOptions = dataStreamOptionsBuilder.build();
                if (dataStreamOptions.failureStore() != null && dataStreamOptions.failureStore().lifecycle() != null) {
                    dataStreamOptions.failureStore()
                        .lifecycle()
                        .addWarningHeaderIfDataRetentionNotEffective(globalRetention, isInternalDataStream);
                }
            }
        }
    }

    /**
     * Validate that by changing or adding {@code newTemplate}, there are
     * no unreferenced data streams. Note that this scenario is still possible
     * due to snapshot restores, but this validation is best-effort at template
     * addition/update time
     */
    private static void validateDataStreamsStillReferenced(
        ProjectMetadata project,
        String templateName,
        ComposableIndexTemplate newTemplate
    ) {
        final Set<String> dataStreams = project.dataStreams()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isSystem() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        Function<ProjectMetadata, Set<String>> findUnreferencedDataStreams = meta -> {
            final Set<String> unreferenced = new HashSet<>();
            // For each data stream that we have, see whether it's covered by a different
            // template (which is great), or whether it's now uncovered by any template
            for (String dataStream : dataStreams) {
                final String matchingTemplate = findV2Template(meta, dataStream, false);
                if (matchingTemplate == null) {
                    unreferenced.add(dataStream);
                } else {
                    // We found a template that still matches, great! Buuuuttt... check whether it
                    // is a data stream template, as it's only useful if it has a data stream definition
                    if (meta.templatesV2().get(matchingTemplate).getDataStreamTemplate() == null) {
                        unreferenced.add(dataStream);
                    }
                }
            }
            return unreferenced;
        };

        // Find data streams that are currently unreferenced
        final Set<String> currentlyUnreferenced = findUnreferencedDataStreams.apply(project);

        // Generate a metadata as if the new template were actually in the cluster state
        final ProjectMetadata updatedMetadata = ProjectMetadata.builder(project).put(templateName, newTemplate).build();
        // Find the data streams that would be unreferenced now that the template is updated/added
        final Set<String> newlyUnreferenced = findUnreferencedDataStreams.apply(updatedMetadata);

        // If we found any data streams that used to be covered, but will no longer be covered by
        // changing this template, then blow up with as much helpful information as we can muster
        if (newlyUnreferenced.size() > currentlyUnreferenced.size()) {
            throw new IllegalArgumentException(
                "composable template ["
                    + templateName
                    + "] with index patterns "
                    + newTemplate.indexPatterns()
                    + ", priority ["
                    + newTemplate.priority()
                    + "] "
                    + (newTemplate.getDataStreamTemplate() == null ? "and no data stream configuration " : "")
                    + "would cause data streams "
                    + newlyUnreferenced
                    + " to no longer match a data stream template"
            );
        }
    }

    /**
     * Return a map of v1 template names to their index patterns for v1 templates that would overlap
     * with the given v2 template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV1Templates(
        final ProjectMetadata project,
        final String candidateName,
        final List<String> indexPatterns
    ) {
        // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
        // Determinization is avoided because it can fail or become very costly due to state explosion.
        Automaton v2automaton = Regex.simpleMatchToNonDeterminizedAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> cursor : project.templates().entrySet()) {
            String name = cursor.getKey();
            IndexTemplateMetadata template = cursor.getValue();
            // No need to determinize the automaton, as it is only used to check for intersection with another automaton.
            Automaton v1automaton = Regex.simpleMatchToNonDeterminizedAutomaton(template.patterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v2automaton, v1automaton)) == false) {
                logger.debug(
                    "composable template {} and legacy template {} would overlap: {} <=> {}",
                    candidateName,
                    name,
                    indexPatterns,
                    template.patterns()
                );
                overlappingTemplates.put(name, template.patterns());
            }
        }
        return overlappingTemplates;
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV2Templates(
        final ProjectMetadata project,
        final String candidateName,
        final List<String> indexPatterns
    ) {
        return findConflictingV2Templates(project, candidateName, indexPatterns, false, 0L);
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     *
     * Based on the provided checkPriority and priority parameters this aims to report the overlapping
     * index templates regardless of the priority (ie. checkPriority == false) or otherwise overlapping
     * templates with the same priority as the given priority parameter (this is useful when trying to
     * add a new template, as we don't support multiple overlapping, from an index pattern perspective,
     * index templates with the same priority).
     */
    static Map<String, List<String>> findConflictingV2Templates(
        final ProjectMetadata project,
        final String candidateName,
        final List<String> indexPatterns,
        boolean checkPriority,
        long priority
    ) {
        Automaton v1automaton = Regex.simpleMatchToAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new TreeMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : project.templatesV2().entrySet()) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            Automaton v2automaton = Regex.simpleMatchToAutomaton(template.indexPatterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v1automaton, v2automaton)) == false) {
                if (checkPriority == false || priority == template.priorityOrZero()) {
                    logger.debug(
                        "legacy template {} and composable template {} would overlap: {} <=> {}",
                        candidateName,
                        name,
                        indexPatterns,
                        template.indexPatterns()
                    );
                    overlappingTemplates.put(name, template.indexPatterns());
                }
            }
        }
        // if the candidate was a V2 template that already exists in the cluster state it will "overlap" with itself so remove it from the
        // results
        overlappingTemplates.remove(candidateName);
        return overlappingTemplates;
    }

    /**
     * Remove the given index template from the cluster state. The index template name
     * supports simple regex wildcards for removing multiple index templates at a time.
     */
    public void removeIndexTemplateV2(
        final ProjectId projectId,
        final String[] names,
        final TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "remove-index-template-v2 [" + String.join(",", names) + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata currentProject) {
                    return innerRemoveIndexTemplateV2(currentProject, names);
                }
            },
            masterTimeout
        );
    }

    // Public because it's used by ReservedComposableIndexTemplateAction
    public static ProjectMetadata innerRemoveIndexTemplateV2(ProjectMetadata project, String... names) {
        Set<String> templateNames = new HashSet<>();

        if (names.length > 1) {
            Set<String> missingNames = null;
            for (String name : names) {
                if (project.templatesV2().containsKey(name)) {
                    templateNames.add(name);
                } else {
                    // wildcards are not supported, so if a name with a wildcard is specified then
                    // the else clause gets executed, because template names can't contain a wildcard.
                    if (missingNames == null) {
                        missingNames = new LinkedHashSet<>();
                    }
                    missingNames.add(name);
                }
            }

            if (missingNames != null) {
                throw new IndexTemplateMissingException(String.join(",", missingNames));
            }
        } else {
            final String name = names[0];
            for (String templateName : project.templatesV2().keySet()) {
                if (Regex.simpleMatch(name, templateName)) {
                    templateNames.add(templateName);
                }
            }
            if (templateNames.isEmpty()) {
                // if it's a match all pattern, and no templates are found (we have none), don't
                // fail with index missing...
                boolean isMatchAll = false;
                if (Regex.isMatchAllPattern(name)) {
                    isMatchAll = true;
                }
                if (isMatchAll) {
                    return project;
                } else {
                    throw new IndexTemplateMissingException(name);
                }
            }
        }

        Set<String> dataStreamsUsingTemplates = dataStreamsExclusivelyUsingTemplates(project, templateNames);
        if (dataStreamsUsingTemplates.size() > 0) {
            throw new IllegalArgumentException(
                "unable to remove composable templates "
                    + new TreeSet<>(templateNames)
                    + " as they are in use by a data streams "
                    + new TreeSet<>(dataStreamsUsingTemplates)
            );
        }

        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        for (String templateName : templateNames) {
            logger.info("removing index template [{}]", templateName);
            builder.removeIndexTemplate(templateName);
        }
        return builder.build();
    }

    /**
     * Returns the data stream names that solely match the patterns of the template names that were provided and no
     * other templates. This means that the returned data streams depend on these templates which has implications for
     * these templates, for example they cannot be removed.
     */
    static Set<String> dataStreamsExclusivelyUsingTemplates(final ProjectMetadata projectMetadata, final Set<String> templateNames) {
        Set<String> namePatterns = templateNames.stream()
            .map(templateName -> projectMetadata.templatesV2().get(templateName))
            .filter(Objects::nonNull)
            .map(ComposableIndexTemplate::indexPatterns)
            .flatMap(List::stream)
            .collect(Collectors.toSet());

        return projectMetadata.dataStreams()
            .values()
            .stream()
            // Limit to checking data streams that match any of the templates' index patterns
            .filter(ds -> namePatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, ds.getName())))
            .filter(ds -> {
                // Retrieve the templates that match the data stream name ordered by priority
                List<Tuple<String, ComposableIndexTemplate>> candidates = findV2CandidateTemplates(
                    projectMetadata.templatesV2().entrySet(),
                    ds.getName(),
                    ds.isHidden(),
                    false
                );
                if (candidates.isEmpty()) {
                    throw new IllegalStateException("Data stream " + ds.getName() + " did not match any composable index templates.");
                }

                // Limit data streams that can ONLY use any of the specified templates, we do this by filtering
                // the matching templates that are others than the ones requested and could be a valid template to use.
                return candidates.stream()
                    .noneMatch(
                        template -> templateNames.contains(template.v1()) == false
                            && isGlobalAndHasIndexHiddenSetting(projectMetadata, template.v2(), template.v1()) == false
                    );
            })
            .map(DataStream::getName)
            .collect(Collectors.toSet());
    }

    public void putTemplate(
        final ProjectId projectId,
        final PutRequest request,
        final TimeValue timeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.indexPatterns == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(request.name);

        taskQueue.submitTask(
            "create-index-template [" + request.name + "], cause [" + request.cause + "]",
            new TemplateClusterStateUpdateTask(listener, projectId) {
                @Override
                public ProjectMetadata execute(ProjectMetadata project) throws Exception {
                    validateTemplate(request.settings, request.mappings, indicesService);
                    return innerPutTemplate(project, request, templateBuilder);
                }
            },
            timeout
        );
    }

    // Package visible for testing
    static ProjectMetadata innerPutTemplate(
        ProjectMetadata currentState,
        PutRequest request,
        IndexTemplateMetadata.Builder templateBuilder
    ) {
        // Flag for whether this is updating an existing template or adding a new one
        // TODO: in 8.0+, only allow updating index templates, not adding new ones
        boolean isUpdate = currentState.templates().containsKey(request.name);
        if (request.create && isUpdate) {
            throw new IllegalArgumentException("index_template [" + request.name + "] already exists");
        }
        boolean isUpdateAndPatternsAreUnchanged = isUpdate
            && currentState.templates().get(request.name).patterns().equals(request.indexPatterns);

        Map<String, List<String>> overlaps = findConflictingV2Templates(currentState, request.name, request.indexPatterns);
        if (overlaps.size() > 0) {
            // Be less strict (just a warning) if we're updating an existing template or this is a match-all template
            if (isUpdateAndPatternsAreUnchanged || request.indexPatterns.stream().anyMatch(Regex::isMatchAllPattern)) {
                String warning = String.format(
                    Locale.ROOT,
                    "legacy template [%s] has index patterns %s matching patterns"
                        + " from existing composable templates [%s] with patterns (%s); this template [%s] may be ignored in favor"
                        + " of a composable template at index creation time",
                    request.name,
                    request.indexPatterns,
                    Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                    overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(",")),
                    request.name
                );
                logger.warn(warning);
                HeaderWarning.addWarning(warning);
            } else {
                // Otherwise, this is a hard error, the user should use V2 index templates instead
                String error = String.format(
                    Locale.ROOT,
                    "legacy template [%s] has index patterns %s matching patterns"
                        + " from existing composable templates [%s] with patterns (%s), use composable templates"
                        + " (/_index_template) instead",
                    request.name,
                    request.indexPatterns,
                    Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                    overlaps.entrySet().stream().map(e -> e.getKey() + " => " + e.getValue()).collect(Collectors.joining(","))
                );
                logger.error(error);
                throw new IllegalArgumentException(error);
            }
        }

        templateBuilder.order(request.order);
        templateBuilder.version(request.version);
        templateBuilder.patterns(request.indexPatterns);
        templateBuilder.settings(request.settings);

        if (request.mappings != null) {
            try {
                templateBuilder.putMapping(MapperService.SINGLE_MAPPING_NAME, request.mappings);
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping: {}", e, request.mappings);
            }
        }

        for (Alias alias : request.aliases) {
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name())
                .filter(alias.filter())
                .indexRouting(alias.indexRouting())
                .searchRouting(alias.searchRouting())
                .writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden())
                .build();
            templateBuilder.putAlias(aliasMetadata);
        }
        IndexTemplateMetadata template = templateBuilder.build();
        IndexTemplateMetadata existingTemplate = currentState.templates().get(request.name);
        if (template.equals(existingTemplate)) {
            // The template is unchanged, therefore there is no need for a cluster state update
            return currentState;
        }

        logger.info("adding template [{}] for index patterns {}", request.name, request.indexPatterns);
        return ProjectMetadata.builder(currentState).put(template).build();
    }

    /**
     * A private, local alternative to elements.stream().anyMatch(predicate) for micro-optimization reasons.
     */
    private static <T> boolean anyMatch(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A private, local alternative to elements.stream().noneMatch(predicate) for micro-optimization reasons.
     */
    private static <T> boolean noneMatch(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A private, local alternative to elements.stream().filter(predicate).findFirst() for micro-optimization reasons.
     */
    private static <T> Optional<T> findFirst(final List<T> elements, final Predicate<T> predicate) {
        for (T e : elements) {
            if (predicate.test(e)) {
                return Optional.of(e);
            }
        }
        return Optional.empty();
    }

    /**
     * Finds index templates whose index pattern matched with the given index name. In the case of
     * hidden indices, a template with a match all pattern or global template will not be returned.
     *
     * @param projectMetadata The {@link ProjectMetadata} containing all of the {@link IndexTemplateMetadata} values
     * @param indexName The name of the index that templates are being found for
     * @param isHidden Whether or not the index is known to be hidden. May be {@code null} if the index
     *                 being hidden has not been explicitly requested. When {@code null} if the result
     *                 of template application results in a hidden index, then global templates will
     *                 not be returned
     * @return a list of templates sorted by {@link IndexTemplateMetadata#order()} descending.
     *
     */
    public static List<IndexTemplateMetadata> findV1Templates(
        ProjectMetadata projectMetadata,
        String indexName,
        @Nullable Boolean isHidden
    ) {
        final String resolvedIndexName = IndexNameExpressionResolver.DateMathExpressionResolver.resolveExpression(indexName);
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, resolvedIndexName);
        final List<IndexTemplateMetadata> matchedTemplates = new ArrayList<>();
        for (IndexTemplateMetadata template : projectMetadata.templates().values()) {
            if (isHidden == null || isHidden == Boolean.FALSE) {
                if (anyMatch(template.patterns(), patternMatchPredicate)) {
                    matchedTemplates.add(template);
                }
            } else {
                assert isHidden == Boolean.TRUE;
                final boolean isNotMatchAllTemplate = noneMatch(template.patterns(), Regex::isMatchAllPattern);
                if (isNotMatchAllTemplate) {
                    if (anyMatch(template.patterns(), patternMatchPredicate)) {
                        matchedTemplates.add(template);
                    }
                }
            }
        }
        CollectionUtil.timSort(matchedTemplates, Comparator.comparingInt(IndexTemplateMetadata::order).reversed());

        // this is complex but if the index is not hidden in the create request but is hidden as the result of template application,
        // then we need to exclude global templates
        if (isHidden == null) {
            final Optional<IndexTemplateMetadata> templateWithHiddenSetting = findFirst(
                matchedTemplates,
                template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())
            );
            if (templateWithHiddenSetting.isPresent()) {
                final boolean templatedIsHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(templateWithHiddenSetting.get().settings());
                if (templatedIsHidden) {
                    // remove the global templates
                    matchedTemplates.removeIf(current -> anyMatch(current.patterns(), Regex::isMatchAllPattern));
                }
                // validate that hidden didn't change
                final Optional<IndexTemplateMetadata> templateWithHiddenSettingPostRemoval = findFirst(
                    matchedTemplates,
                    template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())
                );
                if (templateWithHiddenSettingPostRemoval.isEmpty()
                    || templateWithHiddenSetting.get() != templateWithHiddenSettingPostRemoval.get()) {
                    throw new IllegalStateException(
                        "A global index template ["
                            + templateWithHiddenSetting.get().name()
                            + "] defined the index hidden setting, which is not allowed"
                    );
                }
            }
        }
        return Collections.unmodifiableList(matchedTemplates);
    }

    /**
     * Return the name (id) of the highest matching index template for the given index name. In
     * the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    public static String findV2Template(ProjectMetadata projectMetadata, String indexName, boolean isHidden) {
        return findV2Template(projectMetadata, projectMetadata.templatesV2().entrySet(), indexName, isHidden, false);
    }

    /**
     * Return the name (id) of the highest matching index template out of the provided templates (that <i>need</i> to be sorted descending
     * on priority beforehand), or the given index name. In the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    public static String findV2TemplateFromSortedList(
        ProjectMetadata projectMetadata,
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden
    ) {
        return findV2Template(projectMetadata, templates, indexName, isHidden, true);
    }

    /**
     * Return the name (id) of the highest matching index template, out of the provided templates, for the given index name. In
     * the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    private static String findV2Template(
        ProjectMetadata projectMetadata,
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden,
        boolean exitOnFirstMatch
    ) {
        final List<Tuple<String, ComposableIndexTemplate>> candidates = findV2CandidateTemplates(
            templates,
            indexName,
            isHidden,
            exitOnFirstMatch
        );
        if (candidates.isEmpty()) {
            return null;
        }

        ComposableIndexTemplate winner = candidates.get(0).v2();
        String winnerName = candidates.get(0).v1();

        // if the winner template is a global template that specifies the `index.hidden` setting (which is not allowed, so it'd be due to
        // a restored index cluster state that modified a component template used by this global template such that it has this setting)
        // we will fail and the user will have to update the index template and remove this setting or update the corresponding component
        // template that contributes to the index template resolved settings
        if (isGlobalAndHasIndexHiddenSetting(projectMetadata, winner, winnerName)) {
            throw new IllegalStateException(
                "global index template ["
                    + winnerName
                    + "], composed of component templates ["
                    + String.join(",", winner.composedOf())
                    + "] defined the index.hidden setting, which is not allowed"
            );
        }

        return winnerName;
    }

    /**
     * Return an ordered list of the name (id) and composable index templates that would apply to an index. The first
     * one is the winner template that is applied to this index. In the event that no templates are matched,
     * an empty list is returned.
     * @param templates a list of template entries (name, template) - needs to be sorted when {@code exitOnFirstMatch} is {@code true}
     * @param indexName the index (or data stream) name that should be used for matching the index patterns on the templates
     * @param isHidden whether {@code indexName} belongs to a hidden index - this option is redundant for data streams, as backing indices
     *                 of data streams will always be returned, regardless of whether the data stream is hidden or not
     * @param exitOnFirstMatch if true, we return immediately after finding a match. That means that the <code>templates</code>
     *                         parameter needs to be sorted based on priority (descending) for this method to return a sensible result,
     *                         otherwise this method would just return the first template that matches the name, in an unspecified order
     */
    private static List<Tuple<String, ComposableIndexTemplate>> findV2CandidateTemplates(
        Collection<Map.Entry<String, ComposableIndexTemplate>> templates,
        String indexName,
        boolean isHidden,
        boolean exitOnFirstMatch
    ) {
        assert exitOnFirstMatch == false || areTemplatesSorted(templates) : "Expected templates to be sorted";
        final String resolvedIndexName = IndexNameExpressionResolver.DateMathExpressionResolver.resolveExpression(indexName);
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, resolvedIndexName);
        final List<Tuple<String, ComposableIndexTemplate>> candidates = new ArrayList<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : templates) {
            final String name = entry.getKey();
            final ComposableIndexTemplate template = entry.getValue();
            /*
             * We do not ordinarily return match-all templates for hidden indices. But all backing indices for data streams are hidden,
             * and we do want to return even match-all templates for those. Not doing so can result in a situation where a data stream is
             * built with a template that none of its indices match.
             */
            if (anyMatch(template.indexPatterns(), Regex::isMatchAllPattern) && isHidden && template.getDataStreamTemplate() == null) {
                continue;
            }
            if (anyMatch(template.indexPatterns(), patternMatchPredicate)) {
                candidates.add(Tuple.tuple(name, template));
                if (exitOnFirstMatch) {
                    return candidates;
                }
            }
        }

        CollectionUtil.timSort(candidates, Comparator.comparing(candidate -> candidate.v2().priorityOrZero(), Comparator.reverseOrder()));
        return candidates;
    }

    private static boolean areTemplatesSorted(Collection<Map.Entry<String, ComposableIndexTemplate>> templates) {
        ComposableIndexTemplate previousTemplate = null;
        for (Map.Entry<String, ComposableIndexTemplate> template : templates) {
            if (previousTemplate != null && template.getValue().priorityOrZero() > previousTemplate.priorityOrZero()) {
                return false;
            }
            previousTemplate = template.getValue();
        }
        return true;
    }

    // Checks if a global template specifies the `index.hidden` setting. This check is important because a global
    // template shouldn't specify the `index.hidden` setting, we leave it up to the caller to handle this situation.
    private static boolean isGlobalAndHasIndexHiddenSetting(
        ProjectMetadata projectMetadata,
        ComposableIndexTemplate template,
        String templateName
    ) {
        return anyMatch(template.indexPatterns(), Regex::isMatchAllPattern)
            && IndexMetadata.INDEX_HIDDEN_SETTING.exists(resolveSettings(projectMetadata, templateName));
    }

    /**
     * Collect the given v2 template into an ordered list of mappings.
     */
    public static List<CompressedXContent> collectMappings(
        final ProjectMetadata projectMetadata,
        final String templateName,
        final String indexName
    ) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve mappings for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return List.of();
        }

        final Map<String, ComponentTemplate> componentTemplates = projectMetadata.componentTemplates();
        return collectMappings(template, componentTemplates, indexName);
    }

    /**
     * Collect the given v2 template into an ordered list of mappings.
     */
    public static List<CompressedXContent> collectMappings(
        final ComposableIndexTemplate template,
        final Map<String, ComponentTemplate> componentTemplates,
        final String indexName
    ) {
        Objects.requireNonNull(template, "Composable index template must be provided");
        // Check if this is a failure store index, and if it is, discard any template mappings. Failure store mappings are predefined.
        if (template.getDataStreamTemplate() != null && indexName.startsWith(DataStream.FAILURE_STORE_PREFIX)) {
            return List.of(
                DataStreamFailureStoreDefinition.DATA_STREAM_FAILURE_STORE_MAPPING,
                ComposableIndexTemplate.DataStreamTemplate.DATA_STREAM_MAPPING_SNIPPET
            );
        }
        List<CompressedXContent> mappings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::mappings)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedList::new));
        // Add the actual index template's mappings, since it takes the highest precedence
        Optional.ofNullable(template.template()).map(Template::mappings).ifPresent(mappings::add);
        if (template.getDataStreamTemplate() != null && isDataStreamIndex(indexName)) {
            // add a default mapping for the `@timestamp` field, at the lowest precedence, to make bootstrapping data streams more
            // straightforward as all backing indices are required to have a timestamp field
            if (template.getDataStreamTemplate().isAllowCustomRouting()) {
                mappings.add(0, DEFAULT_TIMESTAMP_MAPPING_WITH_ROUTING);
            } else {
                mappings.add(0, DEFAULT_TIMESTAMP_MAPPING_WITHOUT_ROUTING);
            }
        }

        // Only include _timestamp mapping snippet if creating backing index.
        if (isDataStreamIndex(indexName)) {
            // Only if template has data stream definition this should be added and
            // adding this template last, since _timestamp field should have highest precedence:
            if (template.getDataStreamTemplate() != null) {
                mappings.add(ComposableIndexTemplate.DataStreamTemplate.DATA_STREAM_MAPPING_SNIPPET);
            }
        }
        return Collections.unmodifiableList(mappings);
    }

    private static boolean isDataStreamIndex(String indexName) {
        return indexName.startsWith(DataStream.BACKING_INDEX_PREFIX) || indexName.startsWith(DataStream.FAILURE_STORE_PREFIX);
    }

    /**
     * Resolve index settings for the given list of v1 templates, templates are apply in reverse
     * order since they should be provided in order of priority/order
     */
    public static Settings resolveSettings(final List<IndexTemplateMetadata> templates) {
        Settings.Builder templateSettings = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            templateSettings.put(templates.get(i).settings());
        }
        return templateSettings.build();
    }

    /**
     * Resolve the given v2 template into a collected {@link Settings} object
     */
    public static Settings resolveSettings(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve settings for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return Settings.EMPTY;
        }
        return resolveSettings(template, projectMetadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a collected {@link Settings} object
     */
    public static Settings resolveSettings(ComposableIndexTemplate template, Map<String, ComponentTemplate> componentTemplates) {
        Objects.requireNonNull(template, "attempted to resolve settings for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve settings with null component templates");
        List<Settings> componentSettings = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::settings)
            .filter(Objects::nonNull)
            .toList();

        Settings.Builder templateSettings = Settings.builder();
        componentSettings.forEach(templateSettings::put);
        // Add the actual index template's settings to the end, since it takes the highest precedence.
        Optional.ofNullable(template.template()).map(Template::settings).ifPresent(templateSettings::put);
        return templateSettings.build();
    }

    /**
     * Resolve the given v1 templates into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final List<IndexTemplateMetadata> templates) {
        final List<Map<String, AliasMetadata>> resolvedAliases = new ArrayList<>();
        templates.forEach(template -> {
            if (template.aliases() != null) {
                Map<String, AliasMetadata> aliasMeta = new HashMap<>();
                for (Map.Entry<String, AliasMetadata> cursor : template.aliases().entrySet()) {
                    aliasMeta.put(cursor.getKey(), cursor.getValue());
                }
                resolvedAliases.add(aliasMeta);
            }
        });
        return Collections.unmodifiableList(resolvedAliases);
    }

    /**
     * Resolve the given v2 template name into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve aliases for a template [" + templateName + "] that did not exist in the cluster state";
        return resolveAliases(projectMetadata, template);
    }

    /**
     * Resolve the given v2 template into an ordered list of aliases
     */
    static List<Map<String, AliasMetadata>> resolveAliases(final ProjectMetadata projectMetadata, final ComposableIndexTemplate template) {
        if (template == null) {
            return List.of();
        }
        final Map<String, ComponentTemplate> componentTemplates = projectMetadata.componentTemplates();
        return resolveAliases(template, componentTemplates);
    }

    /**
     * Resolve the given v2 template and component templates into an ordered list of aliases
     */
    static List<Map<String, AliasMetadata>> resolveAliases(
        final ComposableIndexTemplate template,
        final Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve aliases for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve aliases with null component templates");
        List<Map<String, AliasMetadata>> aliases = template.composedOf()
            .stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::aliases)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(ArrayList::new));

        // Add the actual index template's aliases to the end if they exist
        Optional.ofNullable(template.template()).map(Template::aliases).ifPresent(aliases::add);

        // Aliases are applied in order, but subsequent alias configuration from the same name is
        // ignored, so in order for the order to be correct, alias configuration should be in order
        // of precedence (with the index template first)
        Collections.reverse(aliases);
        return Collections.unmodifiableList(aliases);
    }

    /**
     * Resolve the given v2 template into a {@link DataStreamLifecycle} object
     */
    @Nullable
    public static DataStreamLifecycle.Builder resolveLifecycle(ProjectMetadata metadata, final String templateName) {
        final ComposableIndexTemplate template = metadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve lifecycle for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return null;
        }
        return resolveLifecycle(template, metadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a {@link DataStreamLifecycle} object
     */
    @Nullable
    public static DataStreamLifecycle.Builder resolveLifecycle(
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve lifecycle for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve lifecycle with null component templates");

        List<DataStreamLifecycle.Template> lifecycles = new ArrayList<>();
        for (String componentTemplateName : template.composedOf()) {
            if (componentTemplates.containsKey(componentTemplateName) == false) {
                continue;
            }
            DataStreamLifecycle.Template lifecycle = componentTemplates.get(componentTemplateName).template().lifecycle();
            if (lifecycle != null) {
                lifecycles.add(lifecycle);
            }
        }
        // The actual index template's lifecycle has the highest precedence.
        if (template.template() != null && template.template().lifecycle() != null) {
            lifecycles.add(template.template().lifecycle());
        }
        return composeDataLifecycles(lifecycles);
    }

    /**
     * This method composes a series of lifecycles to a final one. The lifecycles are getting composed one level deep,
     * meaning that the keys present on the latest lifecycle will override the ones of the others. If a key is missing
     * then it keeps the value of the previous lifecycles. For example, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "enabled": true,
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *     "lifecycle": {
     *       "enabled": true,
     *       "data_retention" : "20d"
     *     }
     *   }
     * ]
     * The result will be { "lifecycle": { "enabled": true, "data_retention" : "20d"}} because the second data retention overrides the
     * first. However, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "enabled": false,
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *   "lifecycle": {
     *      "enabled": true
     *   }
     *   }
     * ]
     * The result will be { "lifecycle": { "enabled": true, "data_retention" : "10d"} } because the latest lifecycle does not have any
     * information on retention.
     * @param lifecycles a sorted list of lifecycles in the order that they will be composed
     * @return the builder that will build the final lifecycle or the template
     */
    @Nullable
    public static DataStreamLifecycle.Builder composeDataLifecycles(List<DataStreamLifecycle.Template> lifecycles) {
        DataStreamLifecycle.Builder builder = null;
        for (DataStreamLifecycle.Template current : lifecycles) {
            if (builder == null) {
                builder = DataStreamLifecycle.builder(current);
            } else {
                builder.composeTemplate(current);
            }
        }
        return builder;
    }

    /**
     * Resolve the given v2 template into a {@link DataStreamOptions.Builder} object that can be built to either a
     * {@link DataStreamOptions} or the equivalent {@link DataStreamOptions.Template}.
     */
    @Nullable
    public static DataStreamOptions.Builder resolveDataStreamOptions(final ProjectMetadata projectMetadata, final String templateName) {
        final ComposableIndexTemplate template = projectMetadata.templatesV2().get(templateName);
        assert template != null
            : "attempted to resolve data stream options for a template [" + templateName + "] that did not exist in the cluster state";
        if (template == null) {
            return null;
        }
        return resolveDataStreamOptions(template, projectMetadata.componentTemplates());
    }

    /**
     * Resolve the provided v2 template and component templates into a {@link DataStreamOptions.Builder} object that can be built to
     * either a {@link DataStreamOptions} or the equivalent {@link DataStreamOptions.Template}.
     */
    @Nullable
    public static DataStreamOptions.Builder resolveDataStreamOptions(
        ComposableIndexTemplate template,
        Map<String, ComponentTemplate> componentTemplates
    ) {
        Objects.requireNonNull(template, "attempted to resolve data stream for a null template");
        Objects.requireNonNull(componentTemplates, "attempted to resolve data stream options with null component templates");

        List<ResettableValue<DataStreamOptions.Template>> dataStreamOptionsList = new ArrayList<>();
        for (String componentTemplateName : template.composedOf()) {
            if (componentTemplates.containsKey(componentTemplateName) == false) {
                continue;
            }
            ResettableValue<DataStreamOptions.Template> dataStreamOptions = componentTemplates.get(componentTemplateName)
                .template()
                .resettableDataStreamOptions();
            if (dataStreamOptions.isDefined()) {
                dataStreamOptionsList.add(dataStreamOptions);
            }
        }
        // The actual index template's data stream options have the highest precedence.
        if (template.template() != null && template.template().resettableDataStreamOptions().isDefined()) {
            dataStreamOptionsList.add(template.template().resettableDataStreamOptions());
        }
        return composeDataStreamOptions(dataStreamOptionsList);
    }

    /**
     * This method composes a series of data streams options to a final one.
     * @param dataStreamOptionsList a sorted list of data stream options in the order that they will be composed
     * @return the final data stream option configuration
     */
    @Nullable
    public static DataStreamOptions.Builder composeDataStreamOptions(
        List<ResettableValue<DataStreamOptions.Template>> dataStreamOptionsList
    ) {
        if (dataStreamOptionsList.isEmpty()) {
            return null;
        }
        DataStreamOptions.Builder builder = null;
        for (ResettableValue<DataStreamOptions.Template> current : dataStreamOptionsList) {
            if (current.isDefined() == false) {
                continue;
            }
            if (current.shouldReset()) {
                builder = null;
            } else {
                DataStreamOptions.Template currentTemplate = current.get();
                if (builder == null) {
                    builder = DataStreamOptions.builder(currentTemplate);
                } else {
                    builder.composeTemplate(currentTemplate);
                }
            }
        }
        return builder;
    }

    /**
     * Given a state and a composable template, validate that the final composite template
     * generated by the composable template and all of its component templates contains valid
     * settings, mappings, and aliases.
     */
    private static void validateCompositeTemplate(
        final ProjectMetadata project,
        final String templateName,
        final ComposableIndexTemplate template,
        final IndicesService indicesService,
        final NamedXContentRegistry xContentRegistry,
        final SystemIndices systemIndices
    ) throws Exception {
        final ProjectMetadata projectMetadataWithTemplate = ProjectMetadata.builder(project).put(templateName, template).build();

        final String temporaryIndexName = "validate-template-" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
        Settings resolvedSettings = resolveSettings(projectMetadataWithTemplate, templateName);

        // use the provided values, otherwise just pick valid dummy values
        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(resolvedSettings);
        int dummyShards = resolvedSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
        );
        int shardReplicas = resolvedSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        // Create the final aggregate settings, which will be used to create the temporary index metadata to validate everything
        Settings finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(resolvedSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        // Validate index metadata (settings)
        final ProjectMetadata projectMetadataWithIndex = ProjectMetadata.builder(projectMetadataWithTemplate)
            .put(
                IndexMetadata.builder(temporaryIndexName)
                    // necessary to pass asserts in ClusterState constructor
                    .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
                    .settings(finalResolvedSettings)
            )
            .build();
        final IndexMetadata tmpIndexMetadata = projectMetadataWithIndex.index(temporaryIndexName);
        indicesService.withTempIndexService(tmpIndexMetadata, tempIndexService -> {
            // Validate aliases
            MetadataCreateIndexService.resolveAndValidateAliases(
                temporaryIndexName,
                Collections.emptySet(),
                MetadataIndexTemplateService.resolveAliases(projectMetadataWithIndex, templateName),
                projectMetadataWithIndex,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                tempIndexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(System.currentTimeMillis()),
                systemIndices::isSystemName
            );

            // triggers inclusion of _timestamp field and its validation:
            String indexName = DataStream.BACKING_INDEX_PREFIX + temporaryIndexName;
            // Parse mappings to ensure they are valid after being composed

            List<CompressedXContent> mappings = collectMappings(projectMetadataWithIndex, templateName, indexName);
            try {
                MapperService mapperService = tempIndexService.mapperService();
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MapperService.MergeReason.INDEX_TEMPLATE);

                if (template.getDataStreamTemplate() != null) {
                    validateTimestampFieldMapping(mapperService.mappingLookup());
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid composite mappings for [" + templateName + "]", e);
            }
            return null;
        });
    }

    static void validateTemplate(Settings validateSettings, CompressedXContent mappings, IndicesService indicesService) throws Exception {
        // Hard to validate settings if they're non-existent, so used empty ones if none were provided
        Settings settings = validateSettings;
        if (settings == null) {
            settings = Settings.EMPTY;
        }

        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {
            // use the provided values, otherwise just pick valid dummy values
            int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            int dummyShards = settings.getAsInt(
                IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
            );
            int shardReplicas = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

            // create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(settings)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(tmpIndexMetadata, Collections.emptyList(), false);
            createdIndex = dummyIndexService.index();

            if (mappings != null) {
                dummyIndexService.mapperService().merge(MapperService.SINGLE_MAPPING_NAME, mappings, MergeReason.INDEX_TEMPLATE);
            }

        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(
                    createdIndex,
                    NO_LONGER_ASSIGNED,
                    " created for parsing template mapping",
                    CloseUtils.NO_SHARDS_CREATED_EXECUTOR,
                    ActionListener.noop()
                );
            }
        }
    }

    private void validate(String name, ComponentTemplate template) {
        validate(name, template.template(), Collections.emptyList());
    }

    private void validate(String name, ComposableIndexTemplate template) {
        validate(name, template.template(), template.indexPatterns());
    }

    private void validate(String name, Template template, List<String> indexPatterns) {
        Optional<Template> maybeTemplate = Optional.ofNullable(template);
        validate(
            name,
            maybeTemplate.map(Template::settings).orElse(Settings.EMPTY),
            indexPatterns,
            maybeTemplate.map(Template::aliases).orElse(emptyMap()).values().stream().map(MetadataIndexTemplateService::toAlias).toList()
        );
    }

    private static Alias toAlias(AliasMetadata aliasMeta) {
        Alias a = new Alias(aliasMeta.alias());
        if (aliasMeta.filter() != null) {
            a.filter(aliasMeta.filter().string());
        }
        a.searchRouting(aliasMeta.searchRouting());
        a.indexRouting(aliasMeta.indexRouting());
        a.isHidden(aliasMeta.isHidden());
        a.writeIndex(aliasMeta.writeIndex());
        return a;
    }

    private void validate(PutRequest putRequest) {
        validate(putRequest.name, putRequest.settings, putRequest.indexPatterns, putRequest.aliases);
    }

    private void validate(String name, @Nullable Settings settings, List<String> indexPatterns, List<Alias> aliases) {
        List<String> validationErrors = new ArrayList<>();
        if (name.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (name.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (name.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (name.contains("*")) {
            validationErrors.add("name must not contain a '*'");
        }
        if (name.startsWith("_")) {
            validationErrors.add("name must not start with '_'");
        }
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            validationErrors.add("name must be lower cased");
        }
        for (String indexPattern : indexPatterns) {
            if (indexPattern.contains(" ")) {
                validationErrors.add("index_patterns [" + indexPattern + "] must not contain a space");
            }
            if (indexPattern.contains(",")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ','");
            }
            if (indexPattern.contains("#")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a '#'");
            }
            if (indexPattern.contains(":")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ':'");
            }
            if (indexPattern.startsWith("_")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not start with '_'");
            }
            if (Strings.validFileNameExcludingAstrix(indexPattern) == false) {
                validationErrors.add(
                    "index_pattern [" + indexPattern + "] must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
                );
            }
        }

        if (settings != null) {
            try {
                // templates must be consistent with regards to dependencies
                indexScopedSettings.validate(settings, true);
            } catch (IllegalArgumentException iae) {
                validationErrors.add(iae.getMessage());
                for (Throwable t : iae.getSuppressed()) {
                    validationErrors.add(t.getMessage());
                }
            }
            List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(settings, true);
            validationErrors.addAll(indexSettingsValidation);
        }

        if (indexPatterns.stream().anyMatch(Regex::isMatchAllPattern)) {
            if (settings != null && IndexMetadata.INDEX_HIDDEN_SETTING.exists(settings)) {
                validationErrors.add("global templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey());
            }
        }

        if (validationErrors.size() > 0) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(name, validationException.getMessage());
        }

        for (Alias alias : aliases) {
            // we validate the alias only partially, as we don't know yet to which index it'll get applied to
            AliasValidator.validateAliasStandalone(alias);
            if (indexPatterns.contains(alias.name())) {
                throw new IllegalArgumentException(
                    "alias [" + alias.name() + "] cannot be the same as any pattern in [" + String.join(", ", indexPatterns) + "]"
                );
            }
        }
    }

    public static class PutRequest {
        final String name;
        final String cause;
        boolean create;
        int order;
        Integer version;
        List<String> indexPatterns;
        Settings settings = Settings.EMPTY;
        CompressedXContent mappings = null;
        List<Alias> aliases = new ArrayList<>();

        public PutRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

        public PutRequest order(int order) {
            this.order = order;
            return this;
        }

        public PutRequest patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public PutRequest create(boolean create) {
            this.create = create;
            return this;
        }

        public PutRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public PutRequest mappings(CompressedXContent mappings) {
            this.mappings = mappings;
            return this;
        }

        public PutRequest aliases(Set<Alias> aliases) {
            this.aliases.addAll(aliases);
            return this;
        }

        public PutRequest version(Integer version) {
            this.version = version;
            return this;
        }
    }
}
