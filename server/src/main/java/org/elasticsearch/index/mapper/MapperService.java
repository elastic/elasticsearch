/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    public static final String DEFAULT_MAPPING = "_default_";
    public static final String SINGLE_MAPPING_NAME = "_doc";
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
        16,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final boolean INDEX_MAPPER_DYNAMIC_DEFAULT = true;
    @Deprecated
    public static final Setting<Boolean> INDEX_MAPPER_DYNAMIC_SETTING = Setting.boolSetting(
        "index.mapper.dynamic",
        INDEX_MAPPER_DYNAMIC_DEFAULT,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    // Deprecated set of meta-fields, for checking if a field is meta, use an instance method isMetadataField instead
    @Deprecated
    public static final Set<String> META_FIELDS_BEFORE_7DOT8 = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("_id", IgnoredFieldMapper.NAME, "_index", "_routing", "_size", "_timestamp", "_ttl", "_type"))
    );

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MapperService.class);
    static final String DEFAULT_MAPPING_ERROR_MESSAGE = "[_default_] mappings are not allowed on new indices and should no "
        + "longer be used. See [https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes-7.0.html"
        + "#default-mapping-not-allowed] for more information.";

    private final IndexAnalyzers indexAnalyzers;
    private final MappingParser mappingParser;
    private final DocumentParser documentParser;
    private final Version indexVersionCreated;
    private final MapperRegistry mapperRegistry;
    private final Supplier<MappingParserContext> parserContextSupplier;

    private volatile String defaultMappingSource;
    private volatile DocumentMapper mapper;
    private volatile DocumentMapper defaultMapper;

    public MapperService(
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        NamedXContentRegistry xContentRegistry,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        BooleanSupplier idFieldDataEnabled,
        ScriptCompiler scriptCompiler
    ) {
        super(indexSettings);
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.indexAnalyzers = indexAnalyzers;
        this.mapperRegistry = mapperRegistry;
        Function<DateFormatter, MappingParserContext> parserContextFunction = dateFormatter -> new MappingParserContext(
            similarityService::getSimilarity,
            mapperRegistry.getMapperParsers()::get,
            mapperRegistry.getRuntimeFieldParsers()::get,
            indexVersionCreated,
            searchExecutionContextSupplier,
            dateFormatter,
            scriptCompiler,
            indexAnalyzers,
            indexSettings,
            idFieldDataEnabled
        );
        this.documentParser = new DocumentParser(
            xContentRegistry,
            dateFormatter -> new MappingParserContext.DynamicTemplateParserContext(parserContextFunction.apply(dateFormatter)),
            indexSettings,
            indexAnalyzers
        );
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperRegistry.getMetadataMapperParsers(
            indexSettings.getIndexVersionCreated()
        );
        this.parserContextSupplier = () -> parserContextFunction.apply(null);
        this.mappingParser = new MappingParser(
            parserContextSupplier,
            metadataMapperParsers,
            this::getMetadataMappers,
            this::resolveDocumentType,
            xContentRegistry
        );

        if (INDEX_MAPPER_DYNAMIC_SETTING.exists(indexSettings.getSettings())
            && indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException("Setting " + INDEX_MAPPER_DYNAMIC_SETTING.getKey() + " was removed after version 6.0.0");
        }
        defaultMappingSource = "{\"_default_\":{}}";
        if (logger.isTraceEnabled()) {
            logger.trace("default mapping source[{}]", defaultMappingSource);
        }
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

    Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> getMetadataMappers(String type) {
        final DocumentMapper existingMapper = documentMapper(type);
        final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperRegistry.getMetadataMapperParsers(
            indexSettings.getIndexVersionCreated()
        );
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
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)
        ) {
            return parser.map();
        }
    }

    /**
     * Update local mapping by applying the incoming mapping that have already been merged with the current one on the master
     */
    public void updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index())
            : "index mismatch: expected " + index() + " but was " + newIndexMetadata.getIndex();

        if (currentIndexMetadata != null && currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
            assert assertNoUpdateRequired(newIndexMetadata);
            return;
        }

        // go over and add the relevant mappings (or update them)
        Set<String> existingMappers = new HashSet<>();
        if (mapper != null) {
            existingMappers.add(mapper.type());
        }
        if (defaultMapper != null) {
            existingMappers.add(DEFAULT_MAPPING);
        }
        final Map<String, DocumentMapper> updatedEntries;
        try {
            Map<String, CompressedXContent> map = new LinkedHashMap<>();
            for (MappingMetadata mappingMetadata : newIndexMetadata.getMappings().values()) {
                map.put(mappingMetadata.type(), mappingMetadata.source());
            }
            Mappings mappings = parseMappings(map, MergeReason.MAPPING_RECOVERY);
            assert assertRefreshIsNotNeeded(this.mapper, mappings);
            updatedEntries = applyMappings(mappings, () -> map.get(DEFAULT_MAPPING).string(), MergeReason.MAPPING_RECOVERY);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        for (DocumentMapper documentMapper : updatedEntries.values()) {
            String mappingType = documentMapper.type();
            MappingMetadata mappingMetadata;
            if (mappingType.equals(MapperService.DEFAULT_MAPPING)) {
                mappingMetadata = newIndexMetadata.defaultMapping();
            } else {
                mappingMetadata = newIndexMetadata.mapping();
                assert mappingType.equals(mappingMetadata.type());
            }
            CompressedXContent incomingMappingSource = mappingMetadata.source();

            String op = existingMappers.contains(mappingType) ? "updated" : "added";
            if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
                logger.debug("[{}] {} mapping [{}], source [{}]", index(), op, mappingType, incomingMappingSource.string());
            } else if (logger.isTraceEnabled()) {
                logger.trace("[{}] {} mapping [{}], source [{}]", index(), op, mappingType, incomingMappingSource.string());
            } else {
                logger.debug("[{}] {} mapping [{}] (source suppressed due to length, use TRACE level if needed)", index(), op, mappingType);
            }
        }
    }

    private boolean assertRefreshIsNotNeeded(DocumentMapper currentMapper, Mappings mappings) {
        Mappings mergedMappings = mergeMappings(currentMapper, mappings, MergeReason.MAPPING_RECOVERY);
        if (mergedMappings.defaultMapping != null) {
            assertRefreshIsNotNeeded(mergedMappings.defaultMapping, DEFAULT_MAPPING, mappings.defaultMapping);
        }
        if (mergedMappings.incomingMapping != null) {
            String type = mergedMappings.incomingMapping.type();
            assertRefreshIsNotNeeded(mergedMappings.incomingMapping, type, mappings.incomingMapping);
        }
        return true;
    }

    private void assertRefreshIsNotNeeded(Mapping mergedMapping, String type, Mapping incomingMapping) {
        // skip the runtime section or removed runtime fields will make the assertion fail
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
        assert mergedMappingSource.equals(incomingMappingSource)
            : "["
                + index()
                + "] parsed mapping, and got different sources\n"
                + "incoming:\n"
                + incomingMappingSource
                + "\nmerged:\n"
                + mergedMappingSource;
    }

    boolean assertNoUpdateRequired(final IndexMetadata newIndexMetadata) {
        MappingMetadata mapping = newIndexMetadata.mapping();
        if (mapping != null) {
            // mapping representations may change between versions (eg text field mappers
            // used to always explicitly serialize analyzers), so we cannot simply check
            // that the incoming mappings are the same as the current ones: we need to
            // parse the incoming mappings into a DocumentMapper and check that its
            // serialization is the same as the existing mapper
            Mapping newMapping = parseMapping(mapping.type(), mapping.source(), false);
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

    public void merge(Map<String, Map<String, Object>> mappings, MergeReason reason) {
        Map<String, CompressedXContent> mappingSourcesCompressed = new LinkedHashMap<>(mappings.size());
        for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
            try {
                mappingSourcesCompressed.put(
                    entry.getKey(),
                    new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(entry.getValue())))
                );
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }
        mergeAndApplyMappings(mappingSourcesCompressed, reason);
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        mergeAndApplyMappings(Collections.singletonMap(type, content), reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        Map<String, CompressedXContent> map = new LinkedHashMap<>();
        for (MappingMetadata mappingMetadata : indexMetadata.getMappings().values()) {
            map.put(mappingMetadata.type(), mappingMetadata.source());
        }
        mergeAndApplyMappings(map, reason);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return mergeAndApplyMappings(Collections.singletonMap(type, mappingSource), reason).get(type);
    }

    private synchronized Map<String, DocumentMapper> mergeAndApplyMappings(Map<String, CompressedXContent> mappings, MergeReason reason) {
        Mappings parsedMappings = parseMappings(mappings, reason);
        Mappings mergedMappings = mergeMappings(this.mapper, parsedMappings, reason);
        return applyMappings(mergedMappings, () -> mappings.get(DEFAULT_MAPPING).string(), reason);
    }

    private synchronized Map<String, DocumentMapper> applyMappings(
        Mappings mergedMappings,
        Supplier<String> defaultMappingSourceSupplier,
        MergeReason reason
    ) {
        String newDefaultMappingSource = null;
        DocumentMapper newDefaultMapper = null;
        DocumentMapper newMapper = null;
        Map<String, DocumentMapper> documentMappers = new LinkedHashMap<>();
        if (mergedMappings.defaultMapping != null) {
            DocumentMapper newDocumentMapper = newDocumentMapper(mergedMappings.defaultMapping, reason);
            newDefaultMappingSource = defaultMappingSourceSupplier.get();
            newDefaultMapper = newDocumentMapper;
            documentMappers.put(DEFAULT_MAPPING, newDocumentMapper);
        }
        if (mergedMappings.incomingMapping != null) {
            DocumentMapper newDocumentMapper = newDocumentMapper(mergedMappings.incomingMapping, reason);
            newMapper = newDocumentMapper;
            documentMappers.put(newDocumentMapper.type(), newDocumentMapper);
        }
        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return documentMappers;
        }

        if (newDefaultMappingSource != null) {
            this.defaultMappingSource = newDefaultMappingSource;
            this.defaultMapper = newDefaultMapper;
        }
        if (newMapper != null) {
            this.mapper = newMapper;
        }
        assert documentMappers.values().stream().allMatch(this::assertSerialization);
        return documentMappers;
    }

    private DocumentMapper newDocumentMapper(Mapping mapping, MergeReason reason) {
        DocumentMapper newMapper = new DocumentMapper(documentParser, mapping);
        newMapper.mapping().getRoot().fixRedundantIncludes();
        newMapper.validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);
        return newMapper;
    }

    private Mappings parseMappings(Map<String, CompressedXContent> mappings, MergeReason reason) {
        Mapping defaultMapping = null;
        String defaultMappingSource = null;
        if (mappings.containsKey(DEFAULT_MAPPING)) {
            // verify we can parse it
            // NOTE: never apply the default here
            try {
                defaultMapping = mappingParser.parse(DEFAULT_MAPPING, mappings.get(DEFAULT_MAPPING));
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, DEFAULT_MAPPING, e.getMessage());
            }
            defaultMappingSource = mappings.get(DEFAULT_MAPPING).string();
        }

        final String defaultMappingSourceOrLastStored;
        if (defaultMappingSource != null) {
            defaultMappingSourceOrLastStored = defaultMappingSource;
        } else {
            defaultMappingSourceOrLastStored = this.defaultMappingSource;
        }

        Mapping incomingMapping = null;
        for (Map.Entry<String, CompressedXContent> entry : mappings.entrySet()) {
            String type = entry.getKey();
            if (type.equals(DEFAULT_MAPPING)) {
                continue;
            }
            if (incomingMapping != null) {
                throw new IllegalArgumentException("Cannot put multiple mappings: " + mappings.keySet());
            }
            final boolean applyDefault =
                // the default was already applied if we are recovering
                reason != MergeReason.MAPPING_RECOVERY
                    // only apply the default mapping if we don't have the type yet
                    && this.mapper == null;

            try {
                incomingMapping = mappingParser.parse(type, entry.getValue(), applyDefault ? defaultMappingSourceOrLastStored : null);
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        return new Mappings(defaultMapping, incomingMapping);
    }

    public Mapping parseMapping(String type, CompressedXContent mappingSource, boolean applyDefault) {
        try {
            return mappingParser.parse(type, mappingSource, applyDefault ? this.defaultMappingSource : null);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, type, e.getMessage());
        }
    }

    private static class Mappings {
        private final Mapping defaultMapping;
        private final Mapping incomingMapping;

        Mappings(Mapping defaultMapping, Mapping incomingMapping) {
            this.defaultMapping = defaultMapping;
            this.incomingMapping = incomingMapping;
        }
    }

    private Mappings mergeMappings(DocumentMapper currentMapper, Mappings mappings, MergeReason reason) {
        Mapping defaultMapping = mappings.defaultMapping;
        if (defaultMapping != null) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
                throw new IllegalArgumentException(DEFAULT_MAPPING_ERROR_MESSAGE);
            } else if (reason == MergeReason.MAPPING_UPDATE) { // only log in case of explicit mapping updates
                deprecationLogger.critical(DeprecationCategory.MAPPINGS, "default_mapping_not_allowed", DEFAULT_MAPPING_ERROR_MESSAGE);
            }
            assert defaultMapping.type().equals(DEFAULT_MAPPING);
        }
        Mapping incomingMapping = mappings.incomingMapping;
        Mapping newMapping = null;
        if (incomingMapping != null) {
            validateTypeName(incomingMapping.type());
            newMapping = mergeMappings(currentMapper, incomingMapping, reason);
        }
        return new Mappings(defaultMapping, newMapping);
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
        Mapping newMapping = parseMapping(mapper.type(), mappingSource, false);
        if (newMapping.toCompressedXContent().equals(mappingSource) == false) {
            throw new IllegalStateException(
                "Mapping serialization result is different from source. \n--> Source ["
                    + mappingSource
                    + "]\n--> Result ["
                    + newMapping.toCompressedXContent()
                    + "]"
            );
        }
        return true;
    }

    static void validateTypeName(String type) {
        if (type.length() == 0) {
            throw new InvalidTypeNameException("mapping type name is empty");
        }
        if (type.length() > 255) {
            throw new InvalidTypeNameException(
                "mapping type name [" + type + "] is too long; limit is length 255 but was [" + type.length() + "]"
            );
        }
        if (type.charAt(0) == '_' && SINGLE_MAPPING_NAME.equals(type) == false) {
            throw new InvalidTypeNameException(
                "mapping type name [" + type + "] can't start with '_' unless it is called [" + SINGLE_MAPPING_NAME + "]"
            );
        }
        if (type.contains("#")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include '#' in it");
        }
        if (type.contains(",")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include ',' in it");
        }
        if (type.charAt(0) == '.') {
            throw new IllegalArgumentException("mapping type name [" + type + "] must not start with a '.'");
        }
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet
     * or no documents have been indexed in the current index yet (which triggers a dynamic mapping update)
     */
    public DocumentMapper documentMapper() {
        return mapper;
    }

    /**
     * Return the {@link DocumentMapper} for the given type. By using the special
     * {@value #DEFAULT_MAPPING} type, you can get a {@link DocumentMapper} for
     * the default mapping.
     */
    public DocumentMapper documentMapper(String type) {
        if (mapper != null && type.equals(mapper.type())) {
            return mapper;
        }
        if (DEFAULT_MAPPING.equals(type)) {
            return defaultMapper;
        }
        return null;
    }

    /**
     * Check that the resolved type can be used for indexing or deletions
     */
    public void validateType(String type) {
        if (mapper == null) {
            return;
        }
        if (type.equals(mapper.type()) == false) {
            throw new IllegalArgumentException("Invalid type: expecting [" + mapper.type() + "] but got [" + type + "]");
        }
    }

    /**
     * Returns {@code true} if the given {@code mappingSource} includes a type
     * as a top-level object.
     */
    public static boolean isMappingSourceTyped(String type, Map<String, Object> mapping) {
        return mapping.size() == 1 && mapping.keySet().iterator().next().equals(type);
    }

    public static boolean isMappingSourceTyped(String type, CompressedXContent mappingSource) {
        Map<String, Object> root = XContentHelper.convertToMap(mappingSource.compressedReference(), true, XContentType.JSON).v2();
        return isMappingSourceTyped(type, root);
    }

    /**
     * If the _type name is _doc and there is no _doc top-level key then this means that we
     * are handling a typeless call. In such a case, we override _doc with the actual type
     * name in the mappings. This allows to use typeless APIs on typed indices.
     */
    public String getTypeForUpdate(String type, CompressedXContent mappingSource) {
        return isMappingSourceTyped(type, mappingSource) == false ? resolveDocumentType(type) : type;
    }

    /**
     * Resolves a type from a mapping-related request into the type that should be used when
     * merging and updating mappings.
     * <p>
     * If the special `_doc` type is provided, then we replace it with the actual type that is
     * being used in the mappings. This allows typeless APIs such as 'index' or 'put mappings'
     * to work against indices with a custom type name.
     */
    public String resolveDocumentType(String type) {
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
        return mappingLookup.getMatchingFieldNames("*")
            .stream()
            .map(mappingLookup::getFieldType)
            .filter(MappedFieldType::eagerGlobalOrdinals)
            .collect(Collectors.toList());
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
