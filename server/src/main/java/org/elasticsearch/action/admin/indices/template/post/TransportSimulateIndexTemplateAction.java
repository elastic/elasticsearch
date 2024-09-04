/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findConflictingV1Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findConflictingV2Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV2Template;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.resolveLifecycle;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.resolveSettings;

public class TransportSimulateIndexTemplateAction extends TransportMasterNodeReadAction<
    SimulateIndexTemplateRequest,
    SimulateIndexTemplateResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final NamedXContentRegistry xContentRegistry;
    private final IndicesService indicesService;
    private final SystemIndices systemIndices;
    private final Set<IndexSettingProvider> indexSettingProviders;
    private final ClusterSettings clusterSettings;
    private final boolean isDslOnlyMode;

    @Inject
    public TransportSimulateIndexTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedXContentRegistry xContentRegistry,
        IndicesService indicesService,
        SystemIndices systemIndices,
        IndexSettingProviders indexSettingProviders
    ) {
        super(
            SimulateIndexTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SimulateIndexTemplateRequest::new,
            indexNameExpressionResolver,
            SimulateIndexTemplateResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexTemplateService = indexTemplateService;
        this.xContentRegistry = xContentRegistry;
        this.indicesService = indicesService;
        this.systemIndices = systemIndices;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
        this.clusterSettings = clusterService.getClusterSettings();
        this.isDslOnlyMode = isDataStreamsLifecycleOnlyMode(clusterService.getSettings());
    }

    @Override
    protected void masterOperation(
        Task task,
        SimulateIndexTemplateRequest request,
        ClusterState state,
        ActionListener<SimulateIndexTemplateResponse> listener
    ) throws Exception {
        final ClusterState stateWithTemplate;
        if (request.getIndexTemplateRequest() != null) {
            // we'll "locally" add the template defined by the user in the cluster state (as if it existed in the system)
            String simulateTemplateToAdd = "simulate_index_template_" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
            // Perform validation for things like typos in component template names
            MetadataIndexTemplateService.validateV2TemplateRequest(
                state.metadata(),
                simulateTemplateToAdd,
                request.getIndexTemplateRequest().indexTemplate()
            );
            stateWithTemplate = removeExistingAbstractions(
                indexTemplateService.addIndexTemplateV2(
                    state,
                    request.getIndexTemplateRequest().create(),
                    simulateTemplateToAdd,
                    request.getIndexTemplateRequest().indexTemplate()
                ),
                request.getIndexName()
            );
        } else {
            stateWithTemplate = removeExistingAbstractions(state, request.getIndexName());
        }

        String matchingTemplate = findV2Template(stateWithTemplate.metadata(), request.getIndexName(), false);
        if (matchingTemplate == null) {
            listener.onResponse(new SimulateIndexTemplateResponse(null, null));
            return;
        }

        final ClusterState tempClusterState = resolveTemporaryState(matchingTemplate, request.getIndexName(), stateWithTemplate);
        ComposableIndexTemplate templateV2 = tempClusterState.metadata().templatesV2().get(matchingTemplate);
        assert templateV2 != null : "the matched template must exist";

        final Template template = resolveTemplate(
            matchingTemplate,
            request.getIndexName(),
            stateWithTemplate,
            isDslOnlyMode,
            xContentRegistry,
            indicesService,
            systemIndices,
            indexSettingProviders
        );

        final Map<String, List<String>> overlapping = new HashMap<>();
        overlapping.putAll(findConflictingV1Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));
        overlapping.putAll(findConflictingV2Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));

        if (request.includeDefaults()) {
            listener.onResponse(
                new SimulateIndexTemplateResponse(
                    template,
                    overlapping,
                    clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING)
                )
            );
        } else {
            listener.onResponse(new SimulateIndexTemplateResponse(template, overlapping));
        }
    }

    /**
     * Removes the alias, data stream, or existing index from the cluster state if it matches the given index name
     */
    private static ClusterState removeExistingAbstractions(ClusterState state, String indexName) {
        Metadata metadata = state.metadata();
        return ClusterState.builder(state)
            .metadata(Metadata.builder(metadata).removeDataStream(indexName).removeAllIndices().build())
            .build();
    }

    @Override
    protected ClusterBlockException checkBlock(SimulateIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Return a temporary cluster state with an index that exists using the
     * matched template's settings
     */
    public static ClusterState resolveTemporaryState(
        final String matchingTemplate,
        final String indexName,
        final ClusterState simulatedState
    ) {
        Settings settings = resolveSettings(simulatedState.metadata(), matchingTemplate);

        // create the index with dummy settings in the cluster state so we can parse and validate the aliases
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(settings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            // handle mixed-cluster states by passing in minTransportVersion to reset event.ingested range to UNKNOWN if an older version
            .eventIngestedRange(getEventIngestedRange(indexName, simulatedState), simulatedState.getMinTransportVersion())
            .settings(dummySettings)
            .build();
        return ClusterState.builder(simulatedState)
            .metadata(Metadata.builder(simulatedState.metadata()).put(indexMetadata, true).build())
            .build();
    }

    /**
     * Take a template and index name as well as state where the template exists, and return a final
     * {@link Template} that represents all the resolved Settings, Mappings, Aliases and Lifecycle
     */
    public static Template resolveTemplate(
        final String matchingTemplate,
        final String indexName,
        final ClusterState simulatedState,
        final boolean isDslOnlyMode,
        final NamedXContentRegistry xContentRegistry,
        final IndicesService indicesService,
        final SystemIndices systemIndices,
        Set<IndexSettingProvider> indexSettingProviders
    ) throws Exception {
        var metadata = simulatedState.getMetadata();
        Settings templateSettings = resolveSettings(simulatedState.metadata(), matchingTemplate);

        List<Map<String, AliasMetadata>> resolvedAliases = MetadataIndexTemplateService.resolveAliases(
            simulatedState.metadata(),
            matchingTemplate
        );

        ComposableIndexTemplate template = simulatedState.metadata().templatesV2().get(matchingTemplate);
        // create the index with dummy settings in the cluster state so we can parse and validate the aliases
        Settings.Builder dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        // empty request mapping as the user can't specify any explicit mappings via the simulate api
        List<CompressedXContent> mappings = MetadataCreateIndexService.collectV2Mappings(
            null,
            simulatedState,
            matchingTemplate,
            xContentRegistry,
            indexName
        );

        // First apply settings sourced from index settings providers
        final var now = Instant.now();
        Settings.Builder additionalSettings = Settings.builder();
        for (var provider : indexSettingProviders) {
            Settings result = provider.getAdditionalIndexSettings(
                indexName,
                template.getDataStreamTemplate() != null ? indexName : null,
                template.getDataStreamTemplate() != null && metadata.isTimeSeriesTemplate(template),
                simulatedState.getMetadata(),
                now,
                templateSettings,
                mappings
            );
            dummySettings.put(result);
            additionalSettings.put(result);
        }
        // Then apply settings resolved from templates:
        dummySettings.put(templateSettings);

        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            // handle mixed-cluster states by passing in minTransportVersion to reset event.ingested range to UNKNOWN if an older version
            .eventIngestedRange(getEventIngestedRange(indexName, simulatedState), simulatedState.getMinTransportVersion())
            .settings(dummySettings)
            .build();

        final ClusterState tempClusterState = ClusterState.builder(simulatedState)
            .metadata(Metadata.builder(simulatedState.metadata()).put(indexMetadata, true).build())
            .build();

        List<AliasMetadata> aliases = indicesService.withTempIndexService(
            indexMetadata,
            tempIndexService -> MetadataCreateIndexService.resolveAndValidateAliases(
                indexName,
                Set.of(),
                resolvedAliases,
                tempClusterState.metadata(),
                xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                tempIndexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(),
                systemIndices::isSystemName
            )
        );

        Map<String, AliasMetadata> aliasesByName = aliases.stream().collect(Collectors.toMap(AliasMetadata::getAlias, Function.identity()));

        CompressedXContent mergedMapping = indicesService.<CompressedXContent, Exception>withTempIndexService(
            indexMetadata,
            tempIndexService -> {
                MapperService mapperService = tempIndexService.mapperService();
                mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MapperService.MergeReason.INDEX_TEMPLATE);

                DocumentMapper documentMapper = mapperService.documentMapper();
                return documentMapper != null ? documentMapper.mappingSource() : null;
            }
        );

        Settings settings = Settings.builder().put(additionalSettings.build()).put(templateSettings).build();
        DataStreamLifecycle lifecycle = resolveLifecycle(simulatedState.metadata(), matchingTemplate);
        if (template.getDataStreamTemplate() != null && lifecycle == null && isDslOnlyMode) {
            lifecycle = DataStreamLifecycle.DEFAULT;
        }
        return new Template(settings, mergedMapping, aliasesByName, lifecycle);
    }

    private static IndexLongFieldRange getEventIngestedRange(String indexName, ClusterState simulatedState) {
        final IndexMetadata indexMetadata = simulatedState.metadata().index(indexName);
        return indexMetadata == null ? IndexLongFieldRange.NO_SHARDS : indexMetadata.getEventIngestedRange();
    }
}
