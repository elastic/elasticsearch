/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
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
    private static final List<DeprecationChecker> PLUGIN_CHECKERS = List.of(new MlDeprecationChecker());
    private static final Logger logger = LogManager.getLogger(TransportDeprecationInfoAction.class);

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
        skipTheseDeprecations = DeprecationChecks.SKIP_DEPRECATIONS_SETTING.get(settings);
        nodeDeprecationChecker = new NodeDeprecationChecker();
        clusterDeprecationChecker = new ClusterDeprecationChecker(xContentRegistry);
        resourceDeprecationCheckers = List.of(
            new DataStreamDeprecationChecker(indexNameExpressionResolver),
            new TemplateDeprecationChecker(),
            new IlmPolicyDeprecationChecker()
        );
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DeprecationChecks.SKIP_DEPRECATIONS_SETTING, this::setSkipDeprecations);
    }

    /**
     * This is the function that does the bulk of the logic of taking the appropriate ES dependencies
     * like {@link NodeInfo}, {@link ClusterState}. Alongside these objects and the list of deprecation checks,
     * this function will run through all the checks and build out the final list of issues that exist in the
     * cluster.
     *
     * @param state The cluster state
     * @param indexNameExpressionResolver Used to resolve indices into their concrete names
     * @param request The originating request containing the index expressions to evaluate
     * @param nodeSettingsIssues The response containing the deprecation issues found on each node
     * @param clusterDeprecationChecker The checker that provides the cluster settings deprecations warnings
     * @param pluginSettingIssues this map gets modified to move transform deprecation issues into cluster_settings
     * @param skipTheseDeprecatedSettings the settings that will be removed from cluster metadata and the index metadata of all the
     *                                    indexes specified by indexNames
     * @param resourceDeprecationCheckers these are checkers that take as input the cluster state and return a map from resource type
     *                                    to issues grouped by the resource name.
     * @param transformConfigs the transform configuration that have been already retrieved
     * @return The list of deprecation issues found in the cluster
     */
    public static DeprecationInfoAction.Response createResponse(
        ClusterState state,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DeprecationInfoAction.Request request,
        List<DeprecationIssue> nodeSettingsIssues,
        ClusterDeprecationChecker clusterDeprecationChecker,
        Map<String, List<DeprecationIssue>> pluginSettingIssues,
        List<String> skipTheseDeprecatedSettings,
        List<ResourceDeprecationChecker> resourceDeprecationCheckers,
        List<TransformConfig> transformConfigs
    ) {
        assert Transports.assertNotTransportThread("walking mappings in indexSettingsChecks is expensive");
        // Allow system index access here to prevent deprecation warnings when we call this API
        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(state, request);
        ClusterState stateWithSkippedSettingsRemoved = removeSkippedSettings(state, concreteIndexNames, skipTheseDeprecatedSettings);
        List<DeprecationIssue> clusterSettingsIssues = clusterDeprecationChecker.check(stateWithSkippedSettingsRemoved, transformConfigs);

        Map<String, Map<String, List<DeprecationIssue>>> resourceDeprecationIssues = new HashMap<>();
        for (ResourceDeprecationChecker resourceDeprecationChecker : resourceDeprecationCheckers) {
            Map<String, List<DeprecationIssue>> issues = resourceDeprecationChecker.check(stateWithSkippedSettingsRemoved, request);
            if (issues.isEmpty() == false) {
                resourceDeprecationIssues.put(resourceDeprecationChecker.getName(), issues);
            }
        }

        return new DeprecationInfoAction.Response(
            clusterSettingsIssues,
            nodeSettingsIssues,
            resourceDeprecationIssues,
            pluginSettingIssues
        );
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
        nodeDeprecationChecker.check(client, listener.delegateFailureAndWrap((l, nodeDeprecationIssues) -> {
            transformConfigs(l.delegateFailureAndWrap((ll, transformConfigs) -> {
                DeprecationChecker.Components components = new DeprecationChecker.Components(
                    xContentRegistry,
                    settings,
                    new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN)
                );
                pluginSettingIssues(
                    PLUGIN_CHECKERS,
                    components,
                    new ThreadedActionListener<>(
                        client.threadPool().generic(),
                        ll.map(
                            deprecationIssues -> createResponse(
                                state,
                                indexNameExpressionResolver,
                                request,
                                nodeDeprecationIssues,
                                clusterDeprecationChecker,
                                deprecationIssues,
                                skipTheseDeprecations,
                                List.of(
                                    new IndexDeprecationChecker(indexNameExpressionResolver, indexToTransformIds(transformConfigs)),
                                    new DataStreamDeprecationChecker(indexNameExpressionResolver),
                                    new TemplateDeprecationChecker(),
                                    new IlmPolicyDeprecationChecker()
                                ),
                                transformConfigs
                            )
                        )
                    )
                );
            }));
        }));
    }

    /**
     *
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
        Map<String, IndexMetadata> indicesBuilder = new HashMap<>(state.getMetadata().indices());
        for (String indexName : indexNames) {
            IndexMetadata indexMetadata = state.getMetadata().index(indexName);
            IndexMetadata.Builder filteredIndexMetadataBuilder = new IndexMetadata.Builder(indexMetadata);
            Settings filteredSettings = indexMetadata.getSettings()
                .filter(setting -> Regex.simpleMatch(skipTheseDeprecatedSettings, setting) == false);
            filteredIndexMetadataBuilder.settings(filteredSettings);
            indicesBuilder.put(indexName, filteredIndexMetadataBuilder.build());
        }
        metadataBuilder.componentTemplates(state.metadata().componentTemplates().entrySet().stream().map(entry -> {
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
        metadataBuilder.indexTemplates(state.metadata().templatesV2().entrySet().stream().map(entry -> {
            String templateName = entry.getKey();
            ComposableIndexTemplate indexTemplate = entry.getValue();
            Template template = indexTemplate.template();
            if (templateName == null || template.settings() == null || template.settings().isEmpty()) {
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
            new ThreadedActionListener<>(
                threadPool.generic(),
                currentPageListener.delegateFailureAndWrap((delegate, getTransformConfigResponse) -> {
                    var currentPageOfConfigs = getTransformConfigResponse.getTransformConfigurations().stream();
                    var currentPageSize = currentPage.getFrom() + currentPage.getSize();
                    var totalTransformConfigCount = getTransformConfigResponse.getTransformConfigurationCount();
                    if (totalTransformConfigCount >= currentPageSize) {
                        var nextPage = new PageParams(currentPageSize, PageParams.DEFAULT_SIZE);
                        transformConfigs(
                            nextPage,
                            delegate.map(nextPageOfConfigs -> Stream.concat(currentPageOfConfigs, nextPageOfConfigs))
                        );
                    } else {
                        delegate.onResponse(currentPageOfConfigs);
                    }
                })
            )
        );
    }

    private Map<String, List<String>> indexToTransformIds(List<TransformConfig> transformConfigs) {
        return transformConfigs.stream()
            .collect(
                Collectors.groupingBy(
                    config -> config.getDestination().getIndex(),
                    Collectors.mapping(TransformConfig::getId, Collectors.toList())
                )
            );
    }

}
