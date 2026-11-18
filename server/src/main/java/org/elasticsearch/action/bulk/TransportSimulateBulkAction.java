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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.BytesSource;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

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

    // nothing to track for simulate
    private static final IndexingPressure.CoordinatingExpansionTracker NOOP_EXPANSION_TRACKER =
        new IndexingPressure.CoordinatingExpansionTracker() {
            @Override
            public void add(long bytes) {}

            @Override
            public void close() {}
        };

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
        IndexSettingProviders indexSettingProviders,
        FeatureService featureService
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
            threadPool::relativeTimeInNanos,
            featureService
        );
        this.indicesService = indicesService;
        this.xContentRegistry = xContentRegistry;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
    }

    @Override
    protected IndexingPressure.CoordinatingExpansionTracker trackIngestExpansion(boolean forceExecution) {
        return NOOP_EXPANSION_TRACKER;
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
        MapperService.MergeReason mappingMergeReason = getMergeReason(((SimulateBulkRequest) bulkRequest).getMappingMergeType());
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docRequest = bulkRequest.requests.get(i);
            assert docRequest instanceof IndexRequest : "TransportSimulateBulkAction should only ever be called with IndexRequests";
            IndexRequest request = (IndexRequest) docRequest;
            ValidationResult validationResult = validateMappings(
                componentTemplateSubstitutions,
                indexTemplateSubstitutions,
                mappingAddition,
                request,
                mappingMergeReason
            );
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
                        validationResult.ignoredFields,
                        validationResult.validationException,
                        validationResult.effectiveMapping
                    )
                )
            );
        }
        listener.onResponse(
            new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(relativeStartTimeNanos))
        );
    }

    private MapperService.MergeReason getMergeReason(String mergeType) {
        return Optional.ofNullable(mergeType).map(type -> switch (type) {
            case "index" -> MapperService.MergeReason.MAPPING_UPDATE;
            case "template" -> MapperService.MergeReason.INDEX_TEMPLATE;
            default -> throw new IllegalArgumentException(
                "Unsupported merge type '" + mergeType + "'. Valid values are 'index' and 'template'."
            );
        }).orElse(MapperService.MergeReason.MAPPING_UPDATE);
    }

    /**
     * This creates a temporary index with the mappings of the index in the request, and then attempts to index the source from the request
     * into it. If there is a mapping exception, that exception is returned. On success the returned exception is null.
     * @param componentTemplateSubstitutions The component template definitions to use in place of existing ones for validation
     * @param request The IndexRequest whose source will be validated against the mapping (if it exists) of its index
     * @return a Tuple containing: (1) in v1 the names of any fields that would be ignored upon indexing and (2) in v2 the mapping
     * exception if the source does not match the mappings, otherwise null
     */
    private ValidationResult validateMappings(
        Map<String, ComponentTemplate> componentTemplateSubstitutions,
        Map<String, ComposableIndexTemplate> indexTemplateSubstitutions,
        Map<String, Object> mappingAddition,
        IndexRequest request,
        MapperService.MergeReason mappingMergeReason
    ) {
        ProjectMetadata project = projectResolver.getProjectMetadata(clusterService.state());
        CompressedXContent effectiveMapping = null;
        Exception mappingValidationException = null;
        Collection<String> ignoredFields = List.of();
        IndexAbstraction indexAbstraction = project.getIndicesLookup().get(request.index());
        if (indexAbstraction != null && indexAbstraction.isDataStreamRelated()) {
            // _ingest/_simulate sends op_type index, but a real write to a data stream must use op_type create.
            // For a time_series data stream, create picks the backing index whose time range covers the @timestamp.
            request.opType(DocWriteRequest.OpType.CREATE);
        }
        try {
            if (indexAbstraction != null && componentTemplateSubstitutions.isEmpty() && indexTemplateSubstitutions.isEmpty()) {
                /*
                 * In this case the index exists and we don't have any template overrides. So we can just merge the mappingAddition (which
                 * might not exist) into the existing index mapping.
                 */
                IndexMetadata imd = project.getIndexSafe(indexAbstraction.getWriteIndex(request, project));
                CompressedXContent mappings = Optional.ofNullable(imd.mapping()).map(MappingMetadata::source).orElse(null);
                effectiveMapping = mappingAddition == null ? null : mergeMappings(mappings, mappingAddition);
                ignoredFields = validateUpdatedMappingsFromIndexMetadata(imd, false, effectiveMapping, request, mappingMergeReason);
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
                        project.dataStreams().get(request.index()),
                        isDataStreamsLifecycleOnlyMode(clusterService.getSettings()),
                        xContentRegistry,
                        indicesService,
                        systemIndices,
                        indexSettingProviders
                    );
                    CompressedXContent mappings = template.mappings();
                    effectiveMapping = mergeMappings(mappings, mappingAddition);
                    Settings templateSettings = template.settings();
                    if (indexAbstraction instanceof DataStream dataStream
                        && templateSettings.hasValue(IndexSettings.TIME_SERIES_START_TIME.getKey())) {
                        // The data stream was removed above, so the provider set the time range of a new first backing index.
                        // A real write goes to the backing index whose time range covers the @timestamp, so validate with that range.
                        Settings writeIndexSettings = project.getIndexSafe(dataStream.getWriteIndex(request, project)).getSettings();
                        Set<String> timeRange = Set.of(
                            IndexSettings.TIME_SERIES_START_TIME.getKey(),
                            IndexSettings.TIME_SERIES_END_TIME.getKey()
                        );
                        templateSettings = Settings.builder()
                            .put(templateSettings)
                            .put(writeIndexSettings.filter(timeRange::contains))
                            .build();
                    }
                    ignoredFields = validateUpdatedMappings(templateSettings, mappings, effectiveMapping, request, mappingMergeReason);
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
                        effectiveMapping = mergeMappings(new CompressedXContent(mappingsMap), mappingAddition);
                        Settings templateSettings = MetadataIndexTemplateService.resolveSettings(matchingTemplates);
                        ignoredFields = validateUpdatedMappings(templateSettings, null, effectiveMapping, request, mappingMergeReason);
                    } else {
                        /*
                         * The index matched no templates and had no mapping of its own. If there were component template substitutions
                         * or index template substitutions, they didn't match anything. So just apply the mapping addition if it exists,
                         * and validate.
                         */
                        effectiveMapping = mergeMappings(null, mappingAddition);
                        ignoredFields = validateUpdatedMappings(Settings.EMPTY, null, effectiveMapping, request, mappingMergeReason);
                    }
                }
            }
        } catch (Exception e) {
            mappingValidationException = e;
        }
        return new ValidationResult(effectiveMapping, mappingValidationException, ignoredFields);
    }

    private record ValidationResult(CompressedXContent effectiveMapping, Exception validationException, Collection<String> ignoredFields) {

    }

    /*
     * Validates that when updatedMappings are applied. If any fields would be ignored while indexing, then those field names are returned.
     * Otherwise the returned Collection is empty.
     */
    private Collection<String> validateUpdatedMappings(
        Settings templateSettings,
        @Nullable CompressedXContent originalMappings,
        @Nullable CompressedXContent updatedMappings,
        IndexRequest request,
        MapperService.MergeReason mappingMergeReason
    ) throws IOException {
        // The template settings carry the index mode, which mapping validation needs.
        Settings dummySettings = MetadataIndexTemplateService.temporaryIndexSettings(templateSettings);
        IndexMetadata.Builder originalIndexMetadataBuilder = IndexMetadata.builder(request.index()).settings(dummySettings);
        if (originalMappings != null) {
            originalIndexMetadataBuilder.putMapping(new MappingMetadata(originalMappings));
        }
        final IndexMetadata originalIndexMetadata = originalIndexMetadataBuilder.build();
        return validateUpdatedMappingsFromIndexMetadata(originalIndexMetadata, true, updatedMappings, request, mappingMergeReason);
    }

    private Collection<String> validateUpdatedMappingsFromIndexMetadata(
        IndexMetadata originalIndexMetadata,
        boolean newIndex,
        @Nullable CompressedXContent updatedMappings,
        IndexRequest request,
        MapperService.MergeReason mappingMergeReason
    ) throws IOException {
        IndexMode indexMode = originalIndexMetadata.getIndexMode();
        if (updatedMappings == null && (newIndex == false || indexMode == null || indexMode == IndexMode.STANDARD)) {
            return List.of(); // no validation to do, since a new standard index gets no default mapping
        }
        if (IndexMode.isTsdb(originalIndexMetadata.getIndexMode())) {
            // time_series parsing needs the routing hash (and _tsid with index.dimensions) that a real write computes when routing.
            IndexRouting indexRouting = IndexRouting.fromIndexMetadata(originalIndexMetadata);
            request.preRoutingProcess(indexRouting);
            request.route(indexRouting);
            request.postRoutingProcess(indexRouting);
        }
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            new BytesSource(request.source(), request.getContentType(), request.getIncludeSourceOnError()),
            request.routing(),
            request.getDynamicTemplates(),
            request.getDynamicTemplateParams(),
            XContentMeteringParserDecorator.NOOP,
            request.tsid()
        );
        Engine.Index result = indicesService.withTempIndexService(originalIndexMetadata, indexService -> {
            IndexSettings indexSettings = indexService.getIndexSettings();
            CompressedXContent defaultMapping = newIndex ? indexSettings.getMode().getDefaultMapping(indexSettings) : null;
            // Index creation merges the index mode's default mapping first, e.g. @timestamp and the time_series timestamp check.
            List<CompressedXContent> mappings = Stream.of(defaultMapping, updatedMappings).filter(Objects::nonNull).toList();
            if (mappings.isEmpty()) {
                return null; // a new index without mappings in a mode without a default mapping, so there is nothing to validate
            }
            indexService.mapperService().merge(MapperService.SINGLE_MAPPING_NAME, mappings, mappingMergeReason);
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
