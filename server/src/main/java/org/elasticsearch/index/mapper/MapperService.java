/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.TimeSeriesIdGenerator;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptCompiler;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    public static final Setting<Long> INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.nested_fields.limit", 50L, 0, Property.Dynamic, Property.IndexScope);
    // maximum allowed number of nested json objects across all fields in a single document
    public static final Setting<Long> INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.nested_objects.limit", 10000L, 0, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.total_fields.limit", 1000L, 0, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_DEPTH_LIMIT_SETTING =
        Setting.longSetting("index.mapping.depth.limit", 20L, 1, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING =
        Setting.longSetting("index.mapping.field_name_length.limit", Long.MAX_VALUE, 1L, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.dimension_fields.limit", 16, 0, Property.Dynamic, Property.IndexScope);


    private final IndexAnalyzers indexAnalyzers;
    private final MappingParser mappingParser;
    private final DocumentParser documentParser;
    private final Version indexVersionCreated;
    private final MapperRegistry mapperRegistry;
    private final Supplier<MappingParserContext> parserContextSupplier;

    private volatile DocumentMapper mapper;

    public MapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers, NamedXContentRegistry xContentRegistry,
                         SimilarityService similarityService, MapperRegistry mapperRegistry,
                         Supplier<SearchExecutionContext> searchExecutionContextSupplier, BooleanSupplier idFieldDataEnabled,
                         ScriptCompiler scriptCompiler) {
        super(indexSettings);
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.indexAnalyzers = indexAnalyzers;
        this.mapperRegistry = mapperRegistry;
        Function<DateFormatter, MappingParserContext> parserContextFunction =
            dateFormatter -> new MappingParserContext(similarityService::getSimilarity, mapperRegistry.getMapperParsers()::get,
                mapperRegistry.getRuntimeFieldParsers()::get, indexVersionCreated, searchExecutionContextSupplier, dateFormatter,
                scriptCompiler, indexAnalyzers, indexSettings, idFieldDataEnabled);
        this.documentParser = new DocumentParser(xContentRegistry,
            dateFormatter -> new MappingParserContext.DynamicTemplateParserContext(parserContextFunction.apply(dateFormatter)),
            indexSettings, indexAnalyzers);
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
            mapperRegistry.getMetadataMapperParsers(indexSettings.getIndexVersionCreated());
        this.parserContextSupplier = () -> parserContextFunction.apply(null);
        this.mappingParser = new MappingParser(
            parserContextSupplier,
            metadataMapperParsers,
            this::getMetadataMappers,
            this::resolveDocumentType,
            indexSettings.mode()
        );
    }

    public boolean hasNested() {
        return mappingLookup().hasNested();
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public NamedAnalyzer getNamedAnalyzer(String analyzerName) {
        return this.indexAnalyzers.get(analyzerName);
    }

    public MappingParserContext parserContext() {
        return parserContextSupplier.get();
    }

    /**
     * Exposes a {@link DocumentParser}
     * @return a document parser to be used to parse incoming documents
     */
    public DocumentParser documentParser() {
        return this.documentParser;
    }

    Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> getMetadataMappers() {
        final DocumentMapper existingMapper = mapper;
        final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
            mapperRegistry.getMetadataMapperParsers(indexSettings.getIndexVersionCreated());
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();
        if (existingMapper == null) {
            for (MetadataFieldMapper.TypeParser parser : metadataMapperParsers.values()) {
                MetadataFieldMapper metadataFieldMapper = parser.getDefault(parserContext());
                metadataMappers.put(metadataFieldMapper.getClass(), metadataFieldMapper);
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
        try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)) {
            return parser.map();
        }
    }

    /**
     * Update local mapping by applying the incoming mapping that have already been merged with the current one on the master
     */
    public void updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected " + index()
            + " but was " + newIndexMetadata.getIndex();

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
                this.mapper = newDocumentMapper(incomingMapping, MergeReason.MAPPING_RECOVERY);
            }
            String op = previousMapper != null ? "updated" : "added";
            if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
                logger.debug("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
            } else if (logger.isTraceEnabled()) {
                logger.trace("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
            } else {
                logger.debug("[{}] {} mapping (source suppressed due to length, use TRACE level if needed)",
                    index(), op);
            }
        }
    }

    private boolean assertRefreshIsNotNeeded(DocumentMapper currentMapper,
                                             String type,
                                             Mapping incomingMapping) {
        Mapping mergedMapping = mergeMappings(currentMapper, incomingMapping, MergeReason.MAPPING_RECOVERY);
        //skip the runtime section or removed runtime fields will make the assertion fail
        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(RootObjectMapper.TOXCONTENT_SKIP_RUNTIME, "true"));
        CompressedXContent mergedMappingSource;
        try {
            mergedMappingSource = new CompressedXContent(mergedMapping, XContentType.JSON, params);
        } catch (Exception e) {
            throw new AssertionError("failed to serialize source for type [" + type + "]", e);
        }
        CompressedXContent incomingMappingSource;
        try {
            incomingMappingSource = new CompressedXContent(incomingMapping, XContentType.JSON, params);
        } catch (Exception e) {
            throw new AssertionError("failed to serialize source for type [" + type + "]", e);
        }
        // we used to ask the master to refresh its mappings whenever the result of merging the incoming mappings with the
        // current mappings differs from the incoming mappings. We now rather assert that this situation never happens.
        assert mergedMappingSource.equals(incomingMappingSource) : "[" + index() + "] parsed mapping, and got different sources\n" +
            "incoming:\n" + incomingMappingSource + "\nmerged:\n" + mergedMappingSource;
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
                throw new IllegalStateException("expected current mapping [" + currentSource
                    + "] to be the same as new mapping [" + newSource + "]");
            }
        }
        return true;
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        mergeAndApplyMappings(type, content, reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            mergeAndApplyMappings(mappingMetadata.type(), mappingMetadata.source(), reason);
        }
    }

    public void checkDynamicMappingUpdate(String type, CompressedXContent mappingSource) {
        Mapping incomingMapping = parseMapping(type, mappingSource);
        DocumentMapper oldMapper = this.mapper;
        Mapping newMapping = mergeMappings(oldMapper, incomingMapping, MergeReason.MAPPING_UPDATE_PREFLIGHT);
        newDocumentMapper(newMapping, MergeReason.MAPPING_UPDATE_PREFLIGHT);

        TimeSeriesIdGenerator oldTimeSeriesIdGenerator = oldMapper == null ? null : oldMapper.mapping().getTimeSeriesIdGenerator();
        if (false == Objects.equals(newMapping.getTimeSeriesIdGenerator(), oldTimeSeriesIdGenerator)) {
            throw new IllegalStateException("added a dimension with a dynamic mapping");
        }
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return mergeAndApplyMappings(type, mappingSource, reason);
    }

    private synchronized DocumentMapper mergeAndApplyMappings(String mappingType, CompressedXContent mappingSource, MergeReason reason) {
        Mapping incomingMapping = parseMapping(mappingType, mappingSource);
        Mapping mapping = mergeMappings(this.mapper, incomingMapping, reason);
        DocumentMapper newMapper = newDocumentMapper(mapping, reason);
        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return newMapper;
        }
        this.mapper = newMapper;
        assert assertSerialization(newMapper);
        return newMapper;
    }

    private DocumentMapper newDocumentMapper(Mapping mapping, MergeReason reason) {
        DocumentMapper newMapper = new DocumentMapper(documentParser, mapping);
        newMapper.mapping().getRoot().fixRedundantIncludes();
        newMapper.validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);
        return newMapper;
    }

    public Mapping parseMapping(String mappingType, CompressedXContent mappingSource) {
        try {
            return mappingParser.parse(mappingType, mappingSource);
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
            throw new IllegalStateException("Mapping serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapping.toCompressedXContent() + "]");
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
     * Returns all mapped field types.
     */
    public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
        DocumentMapper mapper = this.mapper;
        if (mapper == null) {
            return Collections.emptySet();
        }
        MappingLookup mappingLookup = mapper.mappers();
        return mappingLookup.getMatchingFieldNames("*").stream().map(mappingLookup::getFieldType)
            .filter(MappedFieldType::eagerGlobalOrdinals).collect(Collectors.toList());
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

    public synchronized List<String> reloadSearchAnalyzers(AnalysisRegistry registry) throws IOException {
        logger.info("reloading search analyzers");
        // refresh indexAnalyzers and search analyzers
        final Map<String, TokenizerFactory> tokenizerFactories = registry.buildTokenizerFactories(indexSettings);
        final Map<String, CharFilterFactory> charFilterFactories = registry.buildCharFilterFactories(indexSettings);
        final Map<String, TokenFilterFactory> tokenFilterFactories = registry.buildTokenFilterFactories(indexSettings);
        final Map<String, Settings> settings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        final List<String> reloadedAnalyzers = new ArrayList<>();
        for (NamedAnalyzer namedAnalyzer : indexAnalyzers.getAnalyzers().values()) {
            if (namedAnalyzer.analyzer() instanceof ReloadableCustomAnalyzer) {
                ReloadableCustomAnalyzer analyzer = (ReloadableCustomAnalyzer) namedAnalyzer.analyzer();
                String analyzerName = namedAnalyzer.name();
                Settings analyzerSettings = settings.get(analyzerName);
                analyzer.reload(analyzerName, analyzerSettings, tokenizerFactories, charFilterFactories, tokenFilterFactories);
                reloadedAnalyzers.add(analyzerName);
            }
        }
        // TODO this should bust the cache somehow. Tracked in https://github.com/elastic/elasticsearch/issues/66722
        return reloadedAnalyzers;
    }
}
