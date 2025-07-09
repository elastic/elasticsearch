/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
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
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
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
    public static final NodeFeature SIMULATE_IGNORED_FIELDS = new NodeFeature("simulate.ignored.fields");

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
        ProjectResolver projectResolver,
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
            projectResolver,
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
        Map<String, ComposableIndexTemplate> indexTemplateSubstitutions = bulkRequest.getIndexTemplateSubstitutions();
        Map<String, Object> mappingAddition = ((SimulateBulkRequest) bulkRequest).getMappingAddition();
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docRequest = bulkRequest.requests.get(i);
            assert docRequest instanceof IndexRequest : "TransportSimulateBulkAction should only ever be called with IndexRequests";
            IndexRequest request = (IndexRequest) docRequest;
            Tuple<Collection<String>, Exception> validationResult = validateMappings(
                componentTemplateSubstitutions,
                indexTemplateSubstitutions,
                mappingAddition,
                request
            );
            Exception mappingValidationException = validationResult.v2();
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
                        validationResult.v1(),
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
     * @param componentTemplateSubstitutions The component template definitions to use in place of existing ones for validation
     * @param request The IndexRequest whose source will be validated against the mapping (if it exists) of its index
     * @return a Tuple containing: (1) in v1 the names of any fields that would be ignored upon indexing and (2) in v2 the mapping
     * exception if the source does not match the mappings, otherwise null
     */
    private Tuple<Collection<String>, Exception> validateMappings(
        Map<String, ComponentTemplate> componentTemplateSubstitutions,
        Map<String, ComposableIndexTemplate> indexTemplateSubstitutions,
        Map<String, Object> mappingAddition,
        IndexRequest request
    ) {
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            request.source(),
            request.getContentType(),
            request.routing(),
            request.getDynamicTemplates(),
            request.getIncludeSourceOnError(),
            XContentMeteringParserDecorator.NOOP
        );

        ProjectMetadata project = projectResolver.getProjectMetadata(clusterService.state());
        Exception mappingValidationException = null;
        Collection<String> ignoredFields = List.of();
        IndexAbstraction indexAbstraction = project.getIndicesLookup().get(request.index());
        try {
            if (indexAbstraction != null
                && componentTemplateSubstitutions.isEmpty()
                && indexTemplateSubstitutions.isEmpty()
                && mappingAddition.isEmpty()) {
                /*
                 * In this case the index exists and we don't have any component template overrides. So we can just use withTempIndexService
                 * to do the mapping validation, using all the existing logic for validation.
                 */
                IndexMetadata imd = project.getIndexSafe(indexAbstraction.getWriteIndex(request, project));
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
                ProjectMetadata.Builder simulatedProjectMetadataBuilder = ProjectMetadata.builder(project);
                if (indexAbstraction != null) {
                    /*
                     * We remove the index or data stream from the cluster state so that we are forced to fall back to the templates to get
                     * mappings.
                     */
                    String indexRequest = request.index();
                    assert indexRequest != null : "Index requests cannot be null in a simulate bulk call";
                    if (indexRequest != null) {
                        simulatedProjectMetadataBuilder.remove(indexRequest);
                        simulatedProjectMetadataBuilder.removeDataStream(indexRequest);
                    }
                }
                if (componentTemplateSubstitutions.isEmpty() == false) {
                    /*
                     * We put the template substitutions into the cluster state. If they have the same name as an existing one, the
                     * existing one is replaced.
                     */
                    Map<String, ComponentTemplate> updatedComponentTemplates = new HashMap<>();
                    updatedComponentTemplates.putAll(project.componentTemplates());
                    updatedComponentTemplates.putAll(componentTemplateSubstitutions);
                    simulatedProjectMetadataBuilder.componentTemplates(updatedComponentTemplates);
                }
                if (indexTemplateSubstitutions.isEmpty() == false) {
                    Map<String, ComposableIndexTemplate> updatedIndexTemplates = new HashMap<>();
                    updatedIndexTemplates.putAll(project.templatesV2());
                    updatedIndexTemplates.putAll(indexTemplateSubstitutions);
                    simulatedProjectMetadataBuilder.indexTemplates(updatedIndexTemplates);
                }
                ProjectMetadata simulatedProjectMetadata = simulatedProjectMetadataBuilder.build();

                String matchingTemplate = findV2Template(simulatedProjectMetadata, request.index(), false);
                if (matchingTemplate != null) {
                    /*
                     * The index matches a v2 template (including possibly one or more of the substitutions passed in). So we use this
                     * template, and then possibly apply the mapping addition if it is not null, and validate.
                     */
                    final Template template = TransportSimulateIndexTemplateAction.resolveTemplate(
                        matchingTemplate,
                        request.index(),
                        simulatedProjectMetadata,
                        isDataStreamsLifecycleOnlyMode(clusterService.getSettings()),
                        xContentRegistry,
                        indicesService,
                        systemIndices,
                        indexSettingProviders
                    );
                    CompressedXContent mappings = template.mappings();
                    CompressedXContent mergedMappings = mergeMappings(mappings, mappingAddition);
                    ignoredFields = validateUpdatedMappings(mappings, mergedMappings, request, sourceToParse);
                } else {
                    List<IndexTemplateMetadata> matchingTemplates = findV1Templates(simulatedProjectMetadata, request.index(), false);
                    if (matchingTemplates.isEmpty() == false) {
                        /*
                         * The index matches v1 mappings. These are not compatible with component_template_substitutions or
                         * index_template_substitutions, but we can apply a mapping_addition.
                         */
                        final Map<String, Object> mappingsMap = MetadataCreateIndexService.parseV1Mappings(
                            "{}",
                            matchingTemplates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()),
                            xContentRegistry
                        );
                        final CompressedXContent combinedMappings = mergeMappings(new CompressedXContent(mappingsMap), mappingAddition);
                        ignoredFields = validateUpdatedMappings(null, combinedMappings, request, sourceToParse);
                    } else if (indexAbstraction != null && mappingAddition.isEmpty() == false) {
                        /*
                         * The index matched no templates of any kind, including the substitutions. But it might have a mapping. So we
                         * merge in the mapping addition if it exists, and validate.
                         */
                        MappingMetadata mappingFromIndex = project.index(indexAbstraction.getName()).mapping();
                        CompressedXContent currentIndexCompressedXContent = mappingFromIndex == null ? null : mappingFromIndex.source();
                        CompressedXContent combinedMappings = mergeMappings(currentIndexCompressedXContent, mappingAddition);
                        ignoredFields = validateUpdatedMappings(null, combinedMappings, request, sourceToParse);
                    } else {
                        /*
                         * The index matched no templates and had no mapping of its own. If there were component template substitutions
                         * or index template substitutions, they didn't match anything. So just apply the mapping addition if it exists,
                         * and validate.
                         */
                        final CompressedXContent combinedMappings = mergeMappings(null, mappingAddition);
                        ignoredFields = validateUpdatedMappings(null, combinedMappings, request, sourceToParse);
                    }
                }
            }
        } catch (Exception e) {
            mappingValidationException = e;
        }
        return Tuple.tuple(ignoredFields, mappingValidationException);
    }

    /*
     * Validates that when updatedMappings are applied. If any fields would be ignored while indexing, then those field names are returned.
     * Otherwise the returned Collection is empty.
     */
    private Collection<String> validateUpdatedMappings(
        @Nullable CompressedXContent originalMappings,
        @Nullable CompressedXContent updatedMappings,
        IndexRequest request,
        SourceToParse sourceToParse
    ) throws IOException {
        if (updatedMappings == null) {
            return List.of(); // no validation to do
        }
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        IndexMetadata.Builder originalIndexMetadataBuilder = IndexMetadata.builder(request.index()).settings(dummySettings);
        if (originalMappings != null) {
            originalIndexMetadataBuilder.putMapping(new MappingMetadata(originalMappings));
        }
        final IndexMetadata originalIndexMetadata = originalIndexMetadataBuilder.build();
        final IndexMetadata updatedIndexMetadata = IndexMetadata.builder(request.index())
            .settings(dummySettings)
            .putMapping(new MappingMetadata(updatedMappings))
            .build();
        Engine.Index result = indicesService.withTempIndexService(originalIndexMetadata, indexService -> {
            indexService.mapperService().merge(updatedIndexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
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
        final Collection<String> ignoredFields;
        if (result == null) {
            ignoredFields = List.of();
        } else {
            List<LuceneDocument> luceneDocuments = result.parsedDoc().docs();
            assert luceneDocuments == null || luceneDocuments.size() == 1 : "Expected a single lucene document from index attempt";
            if (luceneDocuments != null && luceneDocuments.size() == 1) {
                ignoredFields = luceneDocuments.getFirst()
                    .getFields()
                    .stream()
                    .filter(field -> field.name().equals(IgnoredFieldMapper.NAME) && field instanceof StringField)
                    .map(IndexableField::stringValue)
                    .toList();
            } else {
                ignoredFields = List.of();
            }
        }
        return ignoredFields;
    }

    private static CompressedXContent mergeMappings(@Nullable CompressedXContent originalMapping, Map<String, Object> mappingAddition)
        throws IOException {
        Map<String, Object> combinedMappingMap = new HashMap<>();
        if (originalMapping != null) {
            combinedMappingMap.putAll(XContentHelper.convertToMap(originalMapping.uncompressed(), true, XContentType.JSON).v2());
        }
        XContentHelper.update(combinedMappingMap, mappingAddition, true);
        if (combinedMappingMap.isEmpty()) {
            return null;
        } else {
            return convertMappingMapToXContent(combinedMappingMap);
        }
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
    protected Boolean resolveFailureStore(String indexName, ProjectMetadata metadata, long epochMillis) {
        // A simulate bulk request should not change any persistent state in the system, so we never write to the failure store
        return null;
    }

    private static CompressedXContent convertMappingMapToXContent(Map<String, Object> rawAdditionalMapping) throws IOException {
        CompressedXContent compressedXContent;
        if (rawAdditionalMapping == null || rawAdditionalMapping.isEmpty()) {
            compressedXContent = null;
        } else {
            try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, rawAdditionalMapping)) {
                compressedXContent = mappingFromXContent(parser);
            }
        }
        return compressedXContent;
    }

    private static CompressedXContent mappingFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(parser.mapOrdered())));
        } else {
            throw new IllegalArgumentException("Unexpected token: " + token);
        }
    }
}
