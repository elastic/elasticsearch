/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<
    DeprecationInfoAction.Request,
    DeprecationInfoAction.Response> {
    public static final Setting<List<String>> SKIP_DEPRECATIONS_SETTING = Setting.stringListSetting(
        "deprecation.skip_deprecated_settings",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final List<DeprecationChecker> PLUGIN_CHECKERS = List.of(new MlDeprecationChecker());

    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;
    private volatile List<String> skipTheseDeprecations;
    private final NodeDeprecationChecker nodeDeprecationChecker;
    private final ClusterDeprecationChecker clusterDeprecationChecker;
    private final List<ResourceDeprecationChecker> resourceDeprecationCheckers;

    @Inject
    public TransportDeprecationInfoAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            DeprecationInfoAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeprecationInfoAction.Request::new,
            DeprecationInfoAction.Response::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        skipTheseDeprecations = SKIP_DEPRECATIONS_SETTING.get(settings);
        nodeDeprecationChecker = new NodeDeprecationChecker(threadPool);
        clusterDeprecationChecker = new ClusterDeprecationChecker(xContentRegistry);
        resourceDeprecationCheckers = List.of(
            new IndexDeprecationChecker(indexNameExpressionResolver),
            new DataStreamDeprecationChecker(indexNameExpressionResolver),
            new TemplateDeprecationChecker(),
            new IlmPolicyDeprecationChecker()
        );
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SKIP_DEPRECATIONS_SETTING, this::setSkipDeprecations);
    }

    private <T> void setSkipDeprecations(List<String> skipDeprecations) {
        this.skipTheseDeprecations = Collections.unmodifiableList(skipDeprecations);
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(
        Task task,
        final DeprecationInfoAction.Request request,
        ClusterState state,
        final ActionListener<DeprecationInfoAction.Response> listener
    ) {
        PrecomputedData precomputedData = new PrecomputedData();
        try (var refs = new RefCountingListener(checkAndCreateResponse(state, request, precomputedData, listener))) {
            nodeDeprecationChecker.check(client, refs.acquire(precomputedData::setOnceNodeSettingsIssues));
            transformConfigs(refs.acquire(precomputedData::setOnceTransformConfigs));
            DeprecationChecker.Components components = new DeprecationChecker.Components(
                xContentRegistry,
                settings,
                new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN)
            );
            pluginSettingIssues(PLUGIN_CHECKERS, components, refs.acquire(precomputedData::setOncePluginIssues));
        }
    }

    /**
     * This is the function that does the bulk of the logic of combining the necessary dependencies together, including the cluster state,
     * the precalculated information in {@code context} with the remaining checkers such as the cluster setting checker and the resource
     * checkers.This function will run a significant part of the checks and build out the final list of issues that exist in the
     * cluster. Because of that, it's important that it does not run in the transport thread that's why it's combined with
     * {@link #executeInGenericThreadpool(ActionListener)}.
     *
     * @param state                       The cluster state
     * @param request                     The originating request containing the index expressions to evaluate
     * @param precomputedData             Data from remote requests necessary to construct the response
     * @param responseListener            The listener expecting the {@link DeprecationInfoAction.Response}
     * @return The listener that should be executed after all the remote requests have completed and the {@link PrecomputedData}
     * is initialised.
     */
    public ActionListener<Void> checkAndCreateResponse(
        ClusterState state,
        DeprecationInfoAction.Request request,
        PrecomputedData precomputedData,
        ActionListener<DeprecationInfoAction.Response> responseListener
    ) {
        return executeInGenericThreadpool(
            ActionListener.running(
                () -> responseListener.onResponse(
                    checkAndCreateResponse(
                        state,
                        indexNameExpressionResolver,
                        request,
                        skipTheseDeprecations,
                        clusterDeprecationChecker,
                        resourceDeprecationCheckers,
                        precomputedData
                    )
                )
            )
        );
    }

    /**
     * This is the function that does the bulk of the logic of combining the necessary dependencies together, including the cluster state,
     * the precalculated information in {@code context} with the remaining checkers such as the cluster setting checker and the resource
     * checkers.This function will run a significant part of the checks and build out the final list of issues that exist in the
     * cluster. It's important that it does not run in the transport thread that's why it's combined with
     * {@link #checkAndCreateResponse(ClusterState, DeprecationInfoAction.Request, PrecomputedData, ActionListener)}. We keep this separated
     * for testing purposes.
     *
     * @param state                       The cluster state
     * @param indexNameExpressionResolver Used to resolve indices into their concrete names
     * @param request                     The originating request containing the index expressions to evaluate
     * @param skipTheseDeprecatedSettings the settings that will be removed from cluster metadata and the index metadata of all the
     *                                    indexes specified by indexNames
     * @param clusterDeprecationChecker   The checker that provides the cluster settings deprecations warnings
     * @param resourceDeprecationCheckers these are checkers that take as input the cluster state and return a map from resource type
     *                                    to issues grouped by the resource name.
     * @param precomputedData             data from remote requests necessary to construct the response
     * @return The list of deprecation issues found in the cluster
     */
    static DeprecationInfoAction.Response checkAndCreateResponse(
        ClusterState state,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DeprecationInfoAction.Request request,
        List<String> skipTheseDeprecatedSettings,
        ClusterDeprecationChecker clusterDeprecationChecker,
        List<ResourceDeprecationChecker> resourceDeprecationCheckers,
        PrecomputedData precomputedData
    ) {
        assert Transports.assertNotTransportThread("walking mappings in indexSettingsChecks is expensive");
        // Allow system index access here to prevent deprecation warnings when we call this API
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, request);
        ClusterState stateWithSkippedSettingsRemoved = removeSkippedSettings(state, concreteIndexNames, skipTheseDeprecatedSettings);
        List<DeprecationIssue> clusterSettingsIssues = clusterDeprecationChecker.check(
            stateWithSkippedSettingsRemoved,
            precomputedData.transformConfigs()
        );

        Map<String, Map<String, List<DeprecationIssue>>> resourceDeprecationIssues = new HashMap<>();
        for (ResourceDeprecationChecker resourceDeprecationChecker : resourceDeprecationCheckers) {
            Map<String, List<DeprecationIssue>> issues = resourceDeprecationChecker.check(
                stateWithSkippedSettingsRemoved,
                request,
                precomputedData
            );
            if (issues.isEmpty() == false) {
                resourceDeprecationIssues.put(resourceDeprecationChecker.getName(), issues);
            }
        }

        return new DeprecationInfoAction.Response(
            clusterSettingsIssues,
            precomputedData.nodeSettingsIssues(),
            resourceDeprecationIssues,
            precomputedData.pluginIssues()
        );
    }

    /**
     * This class holds the results of remote requests. These can be either checks that require remote requests such as
     * {@code nodeSettingsIssues} and {@code pluginIssues} or metadata needed for more than one types of checks such as
     * {@code transformConfigs}.
     */
    public static class PrecomputedData {
        private final SetOnce<List<DeprecationIssue>> nodeSettingsIssues = new SetOnce<>();
        private final SetOnce<Map<String, List<DeprecationIssue>>> pluginIssues = new SetOnce<>();
        private final SetOnce<List<TransformConfig>> transformConfigs = new SetOnce<>();

        public void setOnceNodeSettingsIssues(List<DeprecationIssue> nodeSettingsIssues) {
            this.nodeSettingsIssues.set(nodeSettingsIssues);
        }

        public void setOncePluginIssues(Map<String, List<DeprecationIssue>> pluginIssues) {
            this.pluginIssues.set(pluginIssues);
        }

        public void setOnceTransformConfigs(List<TransformConfig> transformConfigs) {
            this.transformConfigs.set(transformConfigs);
        }

        public List<DeprecationIssue> nodeSettingsIssues() {
            return nodeSettingsIssues.get();
        }

        public Map<String, List<DeprecationIssue>> pluginIssues() {
            return pluginIssues.get();
        }

        public List<TransformConfig> transformConfigs() {
            return transformConfigs.get();
        }
    }

    /**
     * Removes the skipped settings from the selected indices and the component and index templates.
     * @param state The cluster state to modify
     * @param indexNames The names of the indexes whose settings need to be filtered
     * @param skipTheseDeprecatedSettings The settings that will be removed from cluster metadata and the index metadata of all the
     *                                    indexes specified by indexNames
     * @return A modified cluster state with the given settings removed
     */
    private static ClusterState removeSkippedSettings(ClusterState state, String[] indexNames, List<String> skipTheseDeprecatedSettings) {
        // Short-circuit, no need to reconstruct the cluster state if there are no settings to remove
        if (skipTheseDeprecatedSettings == null || skipTheseDeprecatedSettings.isEmpty()) {
            return state;
        }
        ClusterState.Builder clusterStateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
        metadataBuilder.transientSettings(
            metadataBuilder.transientSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
        );
        metadataBuilder.persistentSettings(
            metadataBuilder.persistentSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
        );
        Map<String, IndexMetadata> indicesBuilder = new HashMap<>(state.getMetadata().getProject().indices());
        for (String indexName : indexNames) {
            IndexMetadata indexMetadata = state.getMetadata().getProject().index(indexName);
            IndexMetadata.Builder filteredIndexMetadataBuilder = new IndexMetadata.Builder(indexMetadata);
            Settings filteredSettings = indexMetadata.getSettings()
                .filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false);
            filteredIndexMetadataBuilder.settings(filteredSettings);
            indicesBuilder.put(indexName, filteredIndexMetadataBuilder.build());
        }
        metadataBuilder.componentTemplates(state.metadata().getProject().componentTemplates().entrySet().stream().map(entry -> {
            String templateName = entry.getKey();
            ComponentTemplate componentTemplate = entry.getValue();
            Template template = componentTemplate.template();
            if (template.settings() == null || template.settings().isEmpty()) {
                return Tuple.tuple(templateName, componentTemplate);
            }
            return Tuple.tuple(
                templateName,
                new ComponentTemplate(
                    Template.builder(template)
                        .settings(template.settings().filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false))
                        .build(),
                    componentTemplate.version(),
                    componentTemplate.metadata(),
                    componentTemplate.deprecated()
                )
            );
        }).collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
        metadataBuilder.indexTemplates(state.metadata().getProject().templatesV2().entrySet().stream().map(entry -> {
            String templateName = entry.getKey();
            ComposableIndexTemplate indexTemplate = entry.getValue();
            Template template = indexTemplate.template();
            if (template == null || template.settings() == null || template.settings().isEmpty()) {
                return Tuple.tuple(templateName, indexTemplate);
            }
            return Tuple.tuple(
                templateName,
                indexTemplate.toBuilder()
                    .template(
                        Template.builder(indexTemplate.template())
                            .settings(
                                indexTemplate.template()
                                    .settings()
                                    .filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false)
                            )
                    )
                    .build()
            );
        }).collect(Collectors.toMap(Tuple::v1, Tuple::v2)));

        metadataBuilder.indices(indicesBuilder);
        clusterStateBuilder.metadata(metadataBuilder);
        return clusterStateBuilder.build();
    }

    static void pluginSettingIssues(
        List<DeprecationChecker> checkers,
        DeprecationChecker.Components components,
        ActionListener<Map<String, List<DeprecationIssue>>> listener
    ) {
        List<DeprecationChecker> enabledCheckers = checkers.stream().filter(c -> c.enabled(components.settings())).toList();
        if (enabledCheckers.isEmpty()) {
            listener.onResponse(Collections.emptyMap());
            return;
        }
        GroupedActionListener<DeprecationChecker.CheckResult> groupedActionListener = new GroupedActionListener<>(
            enabledCheckers.size(),
            listener.delegateFailureAndWrap(
                (l, checkResults) -> l.onResponse(
                    checkResults.stream()
                        .collect(
                            Collectors.toMap(DeprecationChecker.CheckResult::getCheckerName, DeprecationChecker.CheckResult::getIssues)
                        )
                )
            )
        );
        for (DeprecationChecker checker : checkers) {
            checker.check(components, groupedActionListener);
        }
    }

    private void transformConfigs(ActionListener<List<TransformConfig>> transformConfigsListener) {
        transformConfigs(new PageParams(0, PageParams.DEFAULT_SIZE), transformConfigsListener.map(Stream::toList));
    }

    private void transformConfigs(PageParams currentPage, ActionListener<Stream<TransformConfig>> currentPageListener) {
        var request = new GetTransformAction.Request(Metadata.ALL);
        request.setPageParams(currentPage);
        request.setAllowNoResources(true);

        client.execute(
            GetTransformAction.INSTANCE,
            request,
            executeInGenericThreadpool(currentPageListener.delegateFailureAndWrap((delegate, getTransformConfigResponse) -> {
                var currentPageOfConfigs = getTransformConfigResponse.getTransformConfigurations().stream();
                var currentPageSize = currentPage.getFrom() + currentPage.getSize();
                var totalTransformConfigCount = getTransformConfigResponse.getTransformConfigurationCount();
                if (totalTransformConfigCount >= currentPageSize) {
                    var nextPage = new PageParams(currentPageSize, PageParams.DEFAULT_SIZE);
                    transformConfigs(nextPage, delegate.map(nextPageOfConfigs -> Stream.concat(currentPageOfConfigs, nextPageOfConfigs)));
                } else {
                    delegate.onResponse(currentPageOfConfigs);
                }
            }))
        );
    }

    private <T> ActionListener<T> executeInGenericThreadpool(ActionListener<T> listener) {
        return new ThreadedActionListener<>(threadPool.generic(), listener);
    }
}
