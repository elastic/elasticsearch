/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     */
    public enum MergeReason {
        /**
         * Pre-flight check before sending a mapping update to the master
         */
        MAPPING_UPDATE_PREFLIGHT,
        /**
         * Create or update a mapping.
         */
        MAPPING_UPDATE,
        /**
         * Merge mappings from a composable index template.
         */
        INDEX_TEMPLATE,
        /**
         * Recovery of an existing mapping, for instance because of a restart,
         * if a shard was moved to a different node or for administrative
         * purposes.
         */
        MAPPING_RECOVERY
    }

    public static final String SINGLE_MAPPING_NAME = "_doc";
    public static final String TYPE_FIELD_NAME = "_type";
    public static final Setting<Long> INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.nested_fields.limit",
        50L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    // maximum allowed number of nested json objects across all fields in a single document
    public static final Setting<Long> INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.nested_objects.limit",
        10000L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.total_fields.limit",
        1000L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_DEPTH_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.depth.limit",
        20L,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.field_name_length.limit",
        Long.MAX_VALUE,
        1L,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.dimension_fields.limit",
        21,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    private final IndexAnalyzers indexAnalyzers;
    private final MappingParser mappingParser;
    private final DocumentParser documentParser;
    private final IndexVersion indexVersionCreated;
    private final MapperRegistry mapperRegistry;
    private final Supplier<MappingParserContext> mappingParserContextSupplier;
    private volatile DocumentMapper mapper;

    public MapperService(
        ClusterService clusterService,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        XContentParserConfiguration parserConfiguration,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        IdFieldMapper idFieldMapper,
        ScriptCompiler scriptCompiler
    ) {
        this(
            () -> clusterService.state().getMinTransportVersion(),
            indexSettings,
            indexAnalyzers,
            parserConfiguration,
            similarityService,
            mapperRegistry,
            searchExecutionContextSupplier,
            idFieldMapper,
            scriptCompiler
        );
    }

    public MapperService(
        Supplier<TransportVersion> clusterTransportVersion,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        XContentParserConfiguration parserConfiguration,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        IdFieldMapper idFieldMapper,
        ScriptCompiler scriptCompiler
    ) {
        super(indexSettings);
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.indexAnalyzers = indexAnalyzers;
        this.mapperRegistry = mapperRegistry;
        this.mappingParserContextSupplier = () -> new MappingParserContext(
            similarityService::getSimilarity,
            type -> mapperRegistry.getMapperParser(type, indexVersionCreated),
            mapperRegistry.getRuntimeFieldParsers()::get,
            indexVersionCreated,
            clusterTransportVersion,
            searchExecutionContextSupplier,
            scriptCompiler,
            indexAnalyzers,
            indexSettings,
            idFieldMapper
        );
        this.documentParser = new DocumentParser(parserConfiguration, this.mappingParserContextSupplier.get());
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperRegistry.getMetadataMapperParsers(
            indexSettings.getIndexVersionCreated()
        );
        this.mappingParser = new MappingParser(
            mappingParserContextSupplier,
            metadataMapperParsers,
            this::getMetadataMappers,
            this::resolveDocumentType
        );
    }

    public boolean hasNested() {
        return mappingLookup().nestedLookup() != NestedLookup.EMPTY;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public MappingParserContext parserContext() {
        return mappingParserContextSupplier.get();
    }

    /**
     * Exposes a {@link DocumentParser}
     * @return a document parser to be used to parse incoming documents
     */
    public DocumentParser documentParser() {
        return this.documentParser;
    }

    Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> getMetadataMappers() {
        final MappingParserContext mappingParserContext = parserContext();
        final DocumentMapper existingMapper = mapper;
        final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperRegistry.getMetadataMapperParsers(
            indexSettings.getIndexVersionCreated()
        );
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();
        if (existingMapper == null) {
            for (MetadataFieldMapper.TypeParser parser : metadataMapperParsers.values()) {
                MetadataFieldMapper metadataFieldMapper = parser.getDefault(mappingParserContext);
                // A MetadataFieldMapper may choose to not be added to the metadata mappers
                // of an index (eg TimeSeriesIdFieldMapper is only added to time series indices)
                // In this case its TypeParser will return null instead of the MetadataFieldMapper
                // instance.
                if (metadataFieldMapper != null) {
                    metadataMappers.put(metadataFieldMapper.getClass(), metadataFieldMapper);
                }
            }
        } else {
            metadataMappers.putAll(existingMapper.mapping().getMetadataMappersMap());
        }
        return metadataMappers;
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws IOException {
        if ("{}".equals(mappingSource)) {
            // empty JSON is a common default value so it makes sense to optimize for it a little
            return Map.of();
        }
        try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig(xContentRegistry), mappingSource)) {
            return parser.map();
        }
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, CompressedXContent mappingSource)
        throws IOException {
        try (
            InputStream in = CompressorFactory.COMPRESSOR.threadLocalInputStream(mappingSource.compressedReference().streamInput());
            XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig(xContentRegistry), in)
        ) {
            return parser.map();
        }
    }

    private static XContentParserConfiguration parserConfig(NamedXContentRegistry xContentRegistry) {
        return XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
    }

    /**
     * Update local mapping by applying the incoming mapping that have already been merged with the current one on the master
     */
    public void updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        assert newIndexMetadata.getIndex().equals(index())
            : "index mismatch: expected " + index() + " but was " + newIndexMetadata.getIndex();

        if (currentIndexMetadata != null && currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
            assert assertNoUpdateRequired(newIndexMetadata);
            return;
        }

        MappingMetadata newMappingMetadata = newIndexMetadata.mapping();
        if (newMappingMetadata != null) {
            String type = newMappingMetadata.type();
            CompressedXContent incomingMappingSource = newMappingMetadata.source();
            Mapping incomingMapping = parseMapping(type, incomingMappingSource);
            DocumentMapper previousMapper;
            synchronized (this) {
                previousMapper = this.mapper;
                assert assertRefreshIsNotNeeded(previousMapper, type, incomingMapping);
                this.mapper = newDocumentMapper(incomingMapping, MergeReason.MAPPING_RECOVERY, incomingMappingSource);
            }
            String op = previousMapper != null ? "updated" : "added";
            if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
                logger.debug("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
            } else if (logger.isTraceEnabled()) {
                logger.trace("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
            } else {
                logger.debug("[{}] {} mapping (source suppressed due to length, use TRACE level if needed)", index(), op);
            }
        }
    }

    private boolean assertRefreshIsNotNeeded(DocumentMapper currentMapper, String type, Mapping incomingMapping) {
        Mapping mergedMapping = mergeMappings(currentMapper, incomingMapping, MergeReason.MAPPING_RECOVERY);
        // skip the runtime section or removed runtime fields will make the assertion fail
        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(RootObjectMapper.TOXCONTENT_SKIP_RUNTIME, "true"));
        CompressedXContent mergedMappingSource;
        try {
            mergedMappingSource = new CompressedXContent(mergedMapping, params);
        } catch (Exception e) {
            throw new AssertionError("failed to serialize source for type [" + type + "]", e);
        }
        CompressedXContent incomingMappingSource;
        try {
            incomingMappingSource = new CompressedXContent(incomingMapping, params);
        } catch (Exception e) {
            throw new AssertionError("failed to serialize source for type [" + type + "]", e);
        }
        // we used to ask the master to refresh its mappings whenever the result of merging the incoming mappings with the
        // current mappings differs from the incoming mappings. We now rather assert that this situation never happens.
        assert mergedMappingSource.equals(incomingMappingSource)
            : "["
                + index()
                + "] parsed mapping, and got different sources\n"
                + "incoming:\n"
                + incomingMappingSource
                + "\nmerged:\n"
                + mergedMappingSource;
        return true;
    }

    boolean assertNoUpdateRequired(final IndexMetadata newIndexMetadata) {
        MappingMetadata mapping = newIndexMetadata.mapping();
        if (mapping != null) {
            // mapping representations may change between versions (eg text field mappers
            // used to always explicitly serialize analyzers), so we cannot simply check
            // that the incoming mappings are the same as the current ones: we need to
            // parse the incoming mappings into a DocumentMapper and check that its
            // serialization is the same as the existing mapper
            Mapping newMapping = parseMapping(mapping.type(), mapping.source());
            final CompressedXContent currentSource = this.mapper.mappingSource();
            final CompressedXContent newSource = newMapping.toCompressedXContent();
            if (Objects.equals(currentSource, newSource) == false) {
                throw new IllegalStateException(
                    "expected current mapping [" + currentSource + "] to be the same as new mapping [" + newSource + "]"
                );
            }
        }
        return true;
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            merge(mappingMetadata.type(), mappingMetadata.source(), reason);
        }
    }

    public DocumentMapper merge(String type, List<CompressedXContent> mappingSources, MergeReason reason) {
        Explicit<Boolean> subobjects = null;

        final DocumentMapper currentMapper = this.mapper;
        if (currentMapper != null) {
            if (mappingSources.size() == 1 && currentMapper.mappingSource().equals(mappingSources.get(0))) {
                return currentMapper;
            }
            if (currentMapper.mapping().getRoot().subobjectsExplicit()) {
                subobjects = Explicit.explicitBoolean(currentMapper.mapping().getRoot().isEnabled());
            }
        }

        List<Mapping> parsedMappings = new LinkedList<>();
        for (CompressedXContent mappingSource : mappingSources) {
            Mapping mapping = parseMapping(type, mappingSource);
            if (mapping.getRoot().subobjectsExplicit()) {
                boolean explicitSubobjects = mapping.getRoot().subobjects();
                if (subobjects != null && subobjects.value() != explicitSubobjects) {
                    throw new MapperParsingException("Failed to parse mappings: contradicting subobjects settings provided");
                }
                subobjects = Explicit.explicitBoolean(explicitSubobjects);
            }
            parsedMappings.add(mapping);
        }

        if (subobjects != null && subobjects.value() != ObjectMapper.Defaults.SUBOBJECTS.value()) {
            // we need to reparse as parsing is affected by the subobjects setting
            parsedMappings.clear();
            for (CompressedXContent mappingSource : mappingSources) {
                parsedMappings.add(parseMapping(type, mappingSource, subobjects));
            }
        }

        DocumentMapper ret = null;
        for (Mapping parsedMapping : parsedMappings) {
            ret = doMerge(reason, parsedMapping);
        }
        return ret;
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        Explicit<Boolean> subobjects = null;
        final DocumentMapper currentMapper = this.mapper;
        if (currentMapper != null) {
            if (currentMapper.mappingSource().equals(mappingSource)) {
                return currentMapper;
            }
            if (currentMapper.mapping().getRoot().subobjectsExplicit()) {
                subobjects = Explicit.explicitBoolean(currentMapper.mapping().getRoot().isEnabled());
            }
        }
        Mapping incomingMapping = parseMapping(type, mappingSource, subobjects);
        return doMerge(reason, incomingMapping);
    }

    private synchronized DocumentMapper doMerge(MergeReason reason, Mapping incomingMapping) {
        Mapping mapping = mergeMappings(this.mapper, incomingMapping, reason);
        // TODO: In many cases the source here is equal to mappingSource so we need not serialize again.
        // We should identify these cases reliably and save expensive serialization here
        DocumentMapper newMapper = newDocumentMapper(mapping, reason, mapping.toCompressedXContent());
        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return newMapper;
        }
        this.mapper = newMapper;
        assert assertSerialization(newMapper);
        return newMapper;
    }

    private DocumentMapper newDocumentMapper(Mapping mapping, MergeReason reason, CompressedXContent mappingSource) {
        DocumentMapper newMapper = new DocumentMapper(documentParser, mapping, mappingSource);
        newMapper.validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);
        return newMapper;
    }

    public Mapping parseMapping(String mappingType, CompressedXContent mappingSource) {
        return parseMapping(mappingType, mappingSource, null);
    }

    public Mapping parseMapping(String mappingType, CompressedXContent mappingSource, @Nullable Explicit<Boolean> subobjects) {
        try {
            return mappingParser.parse(mappingType, mappingSource, subobjects);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }
    }

    public static Mapping mergeMappings(DocumentMapper currentMapper, Mapping incomingMapping, MergeReason reason) {
        Mapping newMapping;
        if (currentMapper == null) {
            newMapping = incomingMapping;
        } else {
            newMapping = currentMapper.mapping().merge(incomingMapping, reason);
        }
        return newMapping;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        Mapping newMapping = parseMapping(mapper.type(), mappingSource);
        if (newMapping.toCompressedXContent().equals(mappingSource) == false) {
            throw new AssertionError(
                "Mapping serialization result is different from source. \n--> Source ["
                    + mappingSource
                    + "]\n--> Result ["
                    + newMapping.toCompressedXContent()
                    + "]"
            );
        }
        return true;
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet
     * or no documents have been indexed in the current index yet (which triggers a dynamic mapping update)
     */
    public DocumentMapper documentMapper() {
        return mapper;
    }

    /**
     * Returns {@code true} if the given {@code mappingSource} includes a type
     * as a top-level object.
     */
    public static boolean isMappingSourceTyped(String type, Map<String, Object> mapping) {
        return mapping.size() == 1 && mapping.keySet().iterator().next().equals(type);
    }

    /**
     * Resolves a type from a mapping-related request into the type that should be used when
     * merging and updating mappings.
     *
     * If the special `_doc` type is provided, then we replace it with the actual type that is
     * being used in the mappings. This allows typeless APIs such as 'index' or 'put mappings'
     * to work against indices with a custom type name.
     */
    private String resolveDocumentType(String type) {
        if (MapperService.SINGLE_MAPPING_NAME.equals(type)) {
            if (mapper != null) {
                return mapper.type();
            }
        }
        return type;
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public MappedFieldType fieldType(String fullName) {
        return mappingLookup().fieldTypesLookup().get(fullName);
    }

    /**
     * Exposes a snapshot of the mappings for the current index.
     * If no mappings have been registered for the current index, an empty {@link MappingLookup} instance is returned.
     * An index does not have mappings only if it was created without providing mappings explicitly,
     * and no documents have yet been indexed in it.
     */
    public MappingLookup mappingLookup() {
        DocumentMapper mapper = this.mapper;
        return mapper == null ? MappingLookup.EMPTY : mapper.mappers();
    }

    /**
     * Returns field types that have eager global ordinals.
     */
    public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
        DocumentMapper mapper = this.mapper;
        if (mapper == null) {
            return Collections.emptySet();
        }
        MappingLookup mappingLookup = mapper.mappers();
        return mappingLookup.getMatchingFieldNames("*")
            .stream()
            .map(mappingLookup::getFieldType)
            .filter(MappedFieldType::eagerGlobalOrdinals)
            .toList();
    }

    /**
     * Return the index-time analyzer associated with a particular field
     * @param field                     the field name
     * @param unindexedFieldAnalyzer    a function to return an Analyzer for a field with no
     *                                  directly associated index-time analyzer
     */
    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
        return mappingLookup().indexAnalyzer(field, unindexedFieldAnalyzer);
    }

    @Override
    public void close() throws IOException {
        indexAnalyzers.close();
    }

    /**
     * @return Whether a field is a metadata field
     * Deserialization of SearchHit objects sent from pre 7.8 nodes and GetResults objects sent from pre 7.3 nodes,
     * uses this method to divide fields into meta and document fields.
     * TODO: remove in v 9.0
     * @deprecated  Use an instance method isMetadataField instead
     */
    @Deprecated
    public static boolean isMetadataFieldStatic(String fieldName) {
        if (IndicesModule.getBuiltInMetadataFields().contains(fieldName)) {
            return true;
        }
        // if a node had Size Plugin installed, _size field should also be considered a meta-field
        return fieldName.equals("_size");
    }

    /**
     * @return Whether a field is a metadata field.
     * this method considers all mapper plugins
     */
    public boolean isMetadataField(String field) {
        return mapperRegistry.getMetadataMapperParsers(indexVersionCreated).containsKey(field);
    }

    /**
     * @return If this field is defined as a multifield of another field
     */
    public boolean isMultiField(String field) {
        return mappingLookup().isMultiField(field);
    }

    public synchronized List<String> reloadSearchAnalyzers(AnalysisRegistry registry, String resource) throws IOException {
        logger.info("reloading search analyzers");
        // TODO this should bust the cache somehow. Tracked in https://github.com/elastic/elasticsearch/issues/66722
        return indexAnalyzers.reload(registry, indexSettings, resource);
    }

    /**
     * @return Returns all dynamic templates defined in this mapping.
     */
    public DynamicTemplate[] getAllDynamicTemplates() {
        return documentMapper().mapping().getRoot().dynamicTemplates();
    }

}
