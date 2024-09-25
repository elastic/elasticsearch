/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SimulateIngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV1Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV2Template;

/**
 * This action simulates bulk indexing data. Pipelines are executed for all indices that the request routes to, but no data is actually
 * indexed and no state is changed. Unlike TransportBulkAction, this does not push the work out to the nodes where the shards live (since
 * shards are not actually modified).
 */
public class TransportSimulateBulkAction extends TransportAbstractBulkAction {
    public static final NodeFeature SIMULATE_MAPPING_VALIDATION = new NodeFeature("simulate.mapping.validation");
    public static final NodeFeature SIMULATE_MAPPING_VALIDATION_TEMPLATES = new NodeFeature("simulate.mapping.validation.templates");
    public static final NodeFeature SIMULATE_COMPONENT_TEMPLATE_SUBSTITUTIONS = new NodeFeature(
        "simulate.component.template.substitutions"
    );
    private final IndicesService indicesService;
    private final NamedXContentRegistry xContentRegistry;
    private final Set<IndexSettingProvider> indexSettingProviders;

    @Inject
    public TransportSimulateBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        IndicesService indicesService,
        NamedXContentRegistry xContentRegistry,
        IndexSettingProviders indexSettingProviders
    ) {
        super(
            SimulateBulkAction.INSTANCE,
            transportService,
            actionFilters,
            SimulateBulkRequest::new,
            threadPool,
            clusterService,
            ingestService,
            indexingPressure,
            systemIndices,
            threadPool::relativeTimeInNanos
        );
        this.indicesService = indicesService;
        this.xContentRegistry = xContentRegistry;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
    }

    @Override
    protected void doInternalExecute(
        Task task,
        BulkRequest bulkRequest,
        Executor executor,
        ActionListener<BulkResponse> listener,
        long relativeStartTimeNanos
    ) throws IOException {
        assert bulkRequest instanceof SimulateBulkRequest
            : "TransportSimulateBulkAction should only ever be called with a SimulateBulkRequest but got a " + bulkRequest.getClass();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        Map<String, ComponentTemplate> componentTemplateSubstitutions = bulkRequest.getComponentTemplateSubstitutions();
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docRequest = bulkRequest.requests.get(i);
            assert docRequest instanceof IndexRequest : "TransportSimulateBulkAction should only ever be called with IndexRequests";
            IndexRequest request = (IndexRequest) docRequest;
            Exception mappingValidationException = validateMappings(componentTemplateSubstitutions, request);
            responses.set(
                i,
                BulkItemResponse.success(
                    0,
                    DocWriteRequest.OpType.CREATE,
                    new SimulateIndexResponse(
                        request.id(),
                        request.index(),
                        request.version(),
                        request.source(),
                        request.getContentType(),
                        request.getExecutedPipelines(),
                        mappingValidationException
                    )
                )
            );
        }
        listener.onResponse(
            new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(relativeStartTimeNanos))
        );
    }

    /**
     * This creates a temporary index with the mappings of the index in the request, and then attempts to index the source from the request
     * into it. If there is a mapping exception, that exception is returned. On success the returned exception is null.
     * @parem componentTemplateSubstitutions The component template definitions to use in place of existing ones for validation
     * @param request The IndexRequest whose source will be validated against the mapping (if it exists) of its index
     * @return a mapping exception if the source does not match the mappings, otherwise null
     */
    private Exception validateMappings(Map<String, ComponentTemplate> componentTemplateSubstitutions, IndexRequest request) {
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            request.source(),
            request.getContentType(),
            request.routing(),
            request.getDynamicTemplates(),
            XContentMeteringParserDecorator.NOOP
        );

        ClusterState state = clusterService.state();
        Exception mappingValidationException = null;
        IndexAbstraction indexAbstraction = state.metadata().getIndicesLookup().get(request.index());
        try {
            if (indexAbstraction != null && componentTemplateSubstitutions.isEmpty()) {
                /*
                 * In this case the index exists and we don't have any component template overrides. So we can just use withTempIndexService
                 * to do the mapping validation, using all the existing logic for validation.
                 */
                IndexMetadata imd = state.metadata().getIndexSafe(indexAbstraction.getWriteIndex(request, state.metadata()));
                indicesService.withTempIndexService(imd, indexService -> {
                    indexService.mapperService().updateMapping(null, imd);
                    return IndexShard.prepareIndex(
                        indexService.mapperService(),
                        sourceToParse,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        -1,
                        -1,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        Long.MIN_VALUE,
                        false,
                        request.ifSeqNo(),
                        request.ifPrimaryTerm(),
                        0
                    );
                });
            } else {
                /*
                 * The index did not exist, or we have component template substitutions, so we put together the mappings from existing
                 * templates This reproduces a lot of the mapping resolution logic in MetadataCreateIndexService.applyCreateIndexRequest().
                 * However, it does not deal with aliases (since an alias cannot be created if an index does not exist, and this is the
                 * path for when the index does not exist). And it does not deal with system indices since we do not intend for users to
                 * simulate writing to system indices.
                 */
                // First, we remove the index from the cluster state if necessary (since we're going to use the templates)
                ClusterState simulatedState = indexAbstraction == null
                    ? state
                    : new ClusterState.Builder(state).metadata(Metadata.builder(state.metadata()).remove(request.index()).build()).build();

                String matchingTemplate = findV2Template(state.metadata(), request.index(), false);
                if (matchingTemplate != null) {
                    final Template template = TransportSimulateIndexTemplateAction.resolveTemplate(
                        matchingTemplate,
                        request.index(),
                        simulatedState,
                        isDataStreamsLifecycleOnlyMode(clusterService.getSettings()),
                        xContentRegistry,
                        indicesService,
                        systemIndices,
                        indexSettingProviders,
                        componentTemplateSubstitutions
                    );
                    CompressedXContent mappings = template.mappings();
                    if (mappings != null) {
                        MappingMetadata mappingMetadata = new MappingMetadata(mappings);
                        Settings dummySettings = Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                            .build();
                        final IndexMetadata imd = IndexMetadata.builder(request.index())
                            .settings(dummySettings)
                            .putMapping(mappingMetadata)
                            .build();
                        indicesService.withTempIndexService(imd, indexService -> {
                            indexService.mapperService().updateMapping(null, imd);
                            return IndexShard.prepareIndex(
                                indexService.mapperService(),
                                sourceToParse,
                                SequenceNumbers.UNASSIGNED_SEQ_NO,
                                -1,
                                -1,
                                VersionType.INTERNAL,
                                Engine.Operation.Origin.PRIMARY,
                                Long.MIN_VALUE,
                                false,
                                request.ifSeqNo(),
                                request.ifPrimaryTerm(),
                                0
                            );
                        });
                    }
                } else {
                    List<IndexTemplateMetadata> matchingTemplates = findV1Templates(state.metadata(), request.index(), false);
                    final Map<String, Object> mappingsMap = MetadataCreateIndexService.parseV1Mappings(
                        "{}",
                        matchingTemplates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()),
                        xContentRegistry
                    );
                    final CompressedXContent combinedMappings;
                    if (mappingsMap.isEmpty()) {
                        combinedMappings = null;
                    } else {
                        combinedMappings = new CompressedXContent(mappingsMap);
                    }
                    Settings dummySettings = Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        .build();
                    MappingMetadata mappingMetadata = combinedMappings == null ? null : new MappingMetadata(combinedMappings);
                    final IndexMetadata imd = IndexMetadata.builder(request.index())
                        .putMapping(mappingMetadata)
                        .settings(dummySettings)
                        .build();
                    indicesService.withTempIndexService(imd, indexService -> {
                        indexService.mapperService().updateMapping(null, imd);
                        return IndexShard.prepareIndex(
                            indexService.mapperService(),
                            sourceToParse,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            -1,
                            -1,
                            VersionType.INTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            Long.MIN_VALUE,
                            false,
                            request.ifSeqNo(),
                            request.ifPrimaryTerm(),
                            0
                        );
                    });
                }
            }
        } catch (Exception e) {
            mappingValidationException = e;
        }
        return mappingValidationException;
    }

    /*
     * This overrides TransportSimulateBulkAction's getIngestService to allow us to provide an IngestService that handles pipeline
     * substitutions defined in the request.
     */
    @Override
    protected IngestService getIngestService(BulkRequest request) {
        IngestService rawIngestService = super.getIngestService(request);
        return new SimulateIngestService(rawIngestService, request);
    }

    @Override
    protected Boolean resolveFailureStore(String indexName, Metadata metadata, long epochMillis) {
        // A simulate bulk request should not change any persistent state in the system, so we never write to the failure store
        return null;
    }
}
