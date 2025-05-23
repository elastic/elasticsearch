/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     */
    public enum MergeReason {
        /**
         * Pre-flight check before sending a dynamic mapping update to the master
         */
        MAPPING_AUTO_UPDATE_PREFLIGHT {
            @Override
            public boolean isAutoUpdate() {
                return true;
            }
        },
        /**
         * Dynamic mapping updates
         */
        MAPPING_AUTO_UPDATE {
            @Override
            public boolean isAutoUpdate() {
                return true;
            }
        },
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
        MAPPING_RECOVERY;

        public boolean isAutoUpdate() {
            return false;
        }
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
        Property.IndexScope,
        Property.ServerlessPublic
    );

    public static final NodeFeature LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT = new NodeFeature(
        "mapper.logsdb_default_ignore_dynamic_beyond_limit"
    );
    public static final Setting<Boolean> INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING = Setting.boolSetting(
        "index.mapping.total_fields.ignore_dynamic_beyond_limit",
        settings -> {
            boolean isLogsDBIndexMode = IndexSettings.MODE.get(settings) == IndexMode.LOGSDB;
            final IndexVersion indexVersionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
            boolean isNewIndexVersion = indexVersionCreated.between(
                IndexVersions.LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT_BACKPORT,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            ) || indexVersionCreated.onOrAfter(IndexVersions.LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT);
            return String.valueOf(isLogsDBIndexMode && isNewIndexVersion);
        },
        Property.Dynamic,
        Property.IndexScope,
        Property.ServerlessPublic
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
        32_768,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Legacy index setting, kept for 7.x BWC compatibility. This setting has no effect in 8.x. Do not use.
     */
    @Deprecated
    public static final Setting<Boolean> INDEX_MAPPER_DYNAMIC_SETTING = Setting.boolSetting(
        "index.mapper.dynamic",
        true,
        Property.Dynamic,
        Property.IndexScope,
        Property.IndexSettingDeprecatedInV7AndRemovedInV8
    );

    private final IndexAnalyzers indexAnalyzers;
    private final MappingParser mappingParser;
    private final DocumentParser documentParser;
    private final IndexVersion indexVersionCreated;
    private final MapperRegistry mapperRegistry;
    private final Supplier<MappingParserContext> mappingParserContextSupplier;
    private final Function<Query, BitSetProducer> bitSetProducer;
    private final MapperMetrics mapperMetrics;

    private volatile DocumentMapper mapper;
    private volatile long mappingVersion;

    public MapperService(
        ClusterService clusterService,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        XContentParserConfiguration parserConfiguration,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        IdFieldMapper idFieldMapper,
        ScriptCompiler scriptCompiler,
        Function<Query, BitSetProducer> bitSetProducer,
        MapperMetrics mapperMetrics
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
            scriptCompiler,
            bitSetProducer,
            mapperMetrics
        );
    }

    @SuppressWarnings("this-escape")
    public MapperService(
        Supplier<TransportVersion> clusterTransportVersion,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        XContentParserConfiguration parserConfiguration,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<SearchExecutionContext> searchExecutionContextSupplier,
        IdFieldMapper idFieldMapper,
        ScriptCompiler scriptCompiler,
        Function<Query, BitSetProducer> bitSetProducer,
        MapperMetrics mapperMetrics
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
            idFieldMapper,
            bitSetProducer,
            mapperRegistry.getNamespaceValidator()
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
        this.bitSetProducer = bitSetProducer;
        this.mapperMetrics = mapperMetrics;
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
            Mapping incomingMapping = parseMapping(type, MergeReason.MAPPING_UPDATE, incomingMappingSource);
            DocumentMapper previousMapper;
            synchronized (this) {
                previousMapper = this.mapper;
                assert assertRefreshIsNotNeeded(previousMapper, type, incomingMapping);
                this.mapper = newDocumentMapper(incomingMapping, MergeReason.MAPPING_RECOVERY, incomingMappingSource);
                this.mappingVersion = newIndexMetadata.getMappingVersion();
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
        Mapping mergedMapping = mergeMappings(currentMapper, incomingMapping, MergeReason.MAPPING_RECOVERY, indexSettings);
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
            Mapping newMapping = parseMapping(mapping.type(), MergeReason.MAPPING_UPDATE, mapping.source());
            final CompressedXContent currentSource = this.mapper.mappingSource();
            final CompressedXContent newSource = newMapping.toCompressedXContent();
            if (Objects.equals(currentSource, newSource) == false
                && mapper.isSyntheticSourceMalformed(currentSource, indexVersionCreated) == false) {
                throw new IllegalStateException(
                    "expected current mapping [" + currentSource + "] to be the same as new mapping [" + newSource + "]"
                );
            }
        }
        return true;

    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            merge(mappingMetadata.type(), mappingMetadata.source(), reason);
        }
    }

    /**
     * Merging the provided mappings. Actual merging is done in the raw, non-parsed, form of the mappings. This allows to do a proper bulk
     * merge, where parsing is done only when all raw mapping settings are already merged.
     */
    public DocumentMapper merge(String type, List<CompressedXContent> mappingSources, MergeReason reason) {
        final DocumentMapper currentMapper = this.mapper;
        if (currentMapper != null && mappingSources.size() == 1 && currentMapper.mappingSource().equals(mappingSources.get(0))) {
            return currentMapper;
        }

        Map<String, Object> mergedRawMapping = null;
        for (CompressedXContent mappingSource : mappingSources) {
            Map<String, Object> rawMapping = MappingParser.convertToMap(mappingSource);

            // normalize mappings, making sure that all have the provided type as a single root
            if (rawMapping.containsKey(type)) {
                if (rawMapping.size() > 1) {
                    throw new MapperParsingException("cannot merge a map with multiple roots, one of which is [" + type + "]");
                }
            } else {
                rawMapping = Map.of(type, rawMapping);
            }

            if (mergedRawMapping == null) {
                mergedRawMapping = rawMapping;
            } else {
                XContentHelper.merge(type, mergedRawMapping, rawMapping, RawFieldMappingMerge.INSTANCE);
            }
        }
        if (mergedRawMapping != null && mergedRawMapping.size() > 1) {
            throw new MapperParsingException("cannot merge mapping sources with different roots");
        }
        return (mergedRawMapping != null) ? doMerge(type, reason, mergedRawMapping) : null;
    }

    /**
     * A {@link org.elasticsearch.common.xcontent.XContentHelper.CustomMerge} for raw map merges that are suitable for index/field mappings.
     * The default raw map merge algorithm doesn't override values - if there are multiple values for a key, then:
     * <ul>
     *     <li> if the values are of map type, the old and new values will be merged recursively
     *     <li> otherwise, the original value will be maintained
     * </ul>
     * When merging field mappings, we want something else. Specifically:
     * <ul>
     *     <li> within field mappings node (which is nested within a {@code properties} node):
     *     <ul>
     *         <li> if both the base mapping and the mapping to merge into it are of mergeable types (e.g {@code object -> object},
     *         {@code object -> nested}), then we only want to merge specific mapping entries:
     *         <ul>
     *             <li> {@code properties} node - merging fields from both mappings
     *             <li> {@code subobjects} entry - since this setting affects an entire subtree, we need to keep it when merging
     *         </ul>
     *         <li> otherwise, for any couple of non-mergeable types ((e.g {@code object -> long}, {@code long -> long}) - we just want
     *         to replace the entire mappings subtree, let the last one win
     *     </ul>
     *     <li> any other map values that are not encountered within a {@code properties} node (e.g. "_doc", "_meta" or "properties"
     *     itself) - apply recursive merge as the default algorithm would apply
     *     <li> any non-map values - override the value of the base map with the value of the merged map
     * </ul>
     */
    private static class RawFieldMappingMerge implements XContentHelper.CustomMerge {
        private static final XContentHelper.CustomMerge INSTANCE = new RawFieldMappingMerge();

        private static final Set<String> MERGEABLE_OBJECT_TYPES = Set.of(ObjectMapper.CONTENT_TYPE, NestedObjectMapper.CONTENT_TYPE);

        private RawFieldMappingMerge() {}

        @SuppressWarnings("unchecked")
        @Override
        public Object merge(String parent, String key, Object oldValue, Object newValue) {
            if (oldValue instanceof Map && newValue instanceof Map) {
                if ("properties".equals(parent)) {
                    // merging two mappings of the same field, where "key" is the field name
                    Map<String, Object> baseMap = (Map<String, Object>) oldValue;
                    Map<String, Object> mapToMerge = (Map<String, Object>) newValue;
                    if (shouldMergeFieldMappings(baseMap, mapToMerge)) {
                        // if two field mappings are to be merged, we only want to keep some specific entries from the base mapping and
                        // let all others be overridden by the second mapping
                        Map<String, Object> mergedMappings = new HashMap<>();
                        // we must keep the "properties" node, otherwise our merge has no point
                        if (baseMap.containsKey("properties")) {
                            mergedMappings.put("properties", new HashMap<>((Map<String, Object>) baseMap.get("properties")));
                        }
                        // the "subobjects" setting affects an entire subtree and not only locally where it is configured
                        if (baseMap.containsKey("subobjects")) {
                            mergedMappings.put("subobjects", baseMap.get("subobjects"));
                        }
                        // Recursively merge these two field mappings.
                        // Since "key" is an arbitrary field name, for which we only need plain mapping subtrees merge, no need to pass it
                        // to the recursion as it shouldn't affect the merge logic. Specifically, passing a parent may cause merge
                        // failures of fields named "properties". See https://github.com/elastic/elasticsearch/issues/108866
                        XContentHelper.merge(mergedMappings, mapToMerge, INSTANCE);
                        return mergedMappings;
                    } else {
                        // non-mergeable types - replace the entire mapping subtree for this field
                        return mapToMerge;
                    }
                }
                // anything else (e.g. "_doc", "_meta", "properties") - no custom merge, rely on caller merge logic
                // field mapping entries of Map type (like "fields" and "meta") are handled above and should never reach here
                return null;
            } else {
                if (key.equals("required")) {
                    // we look for explicit `_routing.required` settings because we use them to detect contradictions of this setting
                    // that comes from mappings with such that comes from the optional `data_stream` configuration of composable index
                    // templates
                    if ("_routing".equals(parent) && oldValue != newValue) {
                        throw new MapperParsingException("contradicting `_routing.required` settings");
                    }
                }
                return newValue;
            }
        }

        /**
         * Normally, we don't want to merge raw field mappings, however there are cases where we do, for example - two
         * "object" (or "nested") mappings.
         *
         * @param mappings1 first mapping of a field
         * @param mappings2 second mapping of a field
         * @return {@code true} if the second mapping should be merged into the first mapping
         */
        private boolean shouldMergeFieldMappings(Map<String, Object> mappings1, Map<String, Object> mappings2) {
            String type1 = (String) mappings1.get("type");
            if (type1 == null && mappings1.get("properties") != null) {
                type1 = ObjectMapper.CONTENT_TYPE;
            }
            String type2 = (String) mappings2.get("type");
            if (type2 == null && mappings2.get("properties") != null) {
                type2 = ObjectMapper.CONTENT_TYPE;
            }
            if (type1 == null || type2 == null) {
                return false;
            }
            return MERGEABLE_OBJECT_TYPES.contains(type1) && MERGEABLE_OBJECT_TYPES.contains(type2);
        }
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        final DocumentMapper currentMapper = this.mapper;
        if (currentMapper != null && currentMapper.mappingSource().equals(mappingSource)) {
            return currentMapper;
        }
        Map<String, Object> mappingSourceAsMap = MappingParser.convertToMap(mappingSource);
        return doMerge(type, reason, mappingSourceAsMap);
    }

    private DocumentMapper doMerge(String type, MergeReason reason, Map<String, Object> mappingSourceAsMap) {
        Mapping incomingMapping = parseMapping(type, reason, mappingSourceAsMap);
        // TODO: In many cases the source here is equal to mappingSource so we need not serialize again.
        // We should identify these cases reliably and save expensive serialization here
        if (reason == MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT) {
            // only doing a merge without updating the actual #mapper field, no need to synchronize
            Mapping mapping = mergeMappings(this.mapper, incomingMapping, MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT, this.indexSettings);
            return newDocumentMapper(mapping, MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT, mapping.toCompressedXContent());
        } else {
            // synchronized concurrent mapper updates are guaranteed to set merged mappers derived from the mapper value previously read
            // TODO: can we even have concurrent updates here?
            synchronized (this) {
                Mapping mapping = mergeMappings(this.mapper, incomingMapping, reason, this.indexSettings);
                DocumentMapper newMapper = newDocumentMapper(mapping, reason, mapping.toCompressedXContent());
                this.mapper = newMapper;
                assert assertSerialization(newMapper, reason);
                return newMapper;
            }
        }
    }

    private DocumentMapper newDocumentMapper(Mapping mapping, MergeReason reason, CompressedXContent mappingSource) {
        DocumentMapper newMapper = new DocumentMapper(
            documentParser,
            mapping,
            mappingSource,
            indexVersionCreated,
            mapperMetrics,
            index().getName()
        );
        newMapper.validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);
        return newMapper;
    }

    public Mapping parseMapping(String mappingType, MergeReason reason, CompressedXContent mappingSource) {
        try {
            return mappingParser.parse(mappingType, reason, mappingSource);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }
    }

    /**
     * A method to parse mapping from a source in a map form.
     *
     * @param mappingType   the mapping type
     * @param reason        the merge reason to use when merging mappers while building the mapper
     * @param mappingSource mapping source already converted to a map form, but not yet processed otherwise
     * @return a parsed mapping
     */
    public Mapping parseMapping(String mappingType, MergeReason reason, Map<String, Object> mappingSource) {
        try {
            return mappingParser.parse(mappingType, reason, mappingSource);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }
    }

    public static Mapping mergeMappings(
        DocumentMapper currentMapper,
        Mapping incomingMapping,
        MergeReason reason,
        IndexSettings indexSettings
    ) {
        return mergeMappings(currentMapper, incomingMapping, reason, getMaxFieldsToAddDuringMerge(currentMapper, indexSettings, reason));
    }

    private static long getMaxFieldsToAddDuringMerge(DocumentMapper currentMapper, IndexSettings indexSettings, MergeReason reason) {
        if (reason.isAutoUpdate() && indexSettings.isIgnoreDynamicFieldsBeyondLimit()) {
            // If the index setting ignore_dynamic_beyond_limit is enabled,
            // data nodes only add new dynamic fields until the limit is reached while parsing documents to be ingested.
            // However, if there are concurrent mapping updates,
            // data nodes may add dynamic fields under an outdated assumption that enough capacity is still available.
            // When data nodes send the dynamic mapping update request to the master node,
            // it will only add as many fields as there's actually capacity for when merging mappings.
            long totalFieldsLimit = indexSettings.getMappingTotalFieldsLimit();
            return Optional.ofNullable(currentMapper)
                .map(DocumentMapper::mappers)
                .map(ml -> ml.remainingFieldsUntilLimit(totalFieldsLimit))
                .orElse(totalFieldsLimit);
        } else {
            // Else, we're not limiting the number of fields so that the merged mapping fails validation if it exceeds total_fields.limit.
            // This is the desired behavior when making an explicit mapping update, even if ignore_dynamic_beyond_limit is enabled.
            // When ignore_dynamic_beyond_limit is disabled and a dynamic mapping update would exceed the field limit,
            // the document will get rejected.
            // Normally, this happens on the data node in DocumentParserContext.addDynamicMapper but if there's a race condition,
            // data nodes may add dynamic fields under an outdated assumption that enough capacity is still available.
            // In this case, the master node will reject mapping updates that would exceed the limit when handling the mapping update.
            return Long.MAX_VALUE;
        }
    }

    static Mapping mergeMappings(DocumentMapper currentMapper, Mapping incomingMapping, MergeReason reason, long newFieldsBudget) {
        Mapping newMapping;
        if (currentMapper == null) {
            newMapping = incomingMapping.withFieldsBudget(newFieldsBudget);
        } else {
            newMapping = currentMapper.mapping().merge(incomingMapping, reason, newFieldsBudget);
        }
        return newMapping;
    }

    private boolean assertSerialization(DocumentMapper mapper, MergeReason reason) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        Mapping newMapping = parseMapping(mapper.type(), reason, mappingSource);
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

    public long mappingVersion() {
        return mappingVersion;
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
        var mapper = mappingLookup().getMapper(field);
        return mapper instanceof MetadataFieldMapper;
    }

    /**
     * @return If this field is defined as a multifield of another field
     */
    public boolean isMultiField(String field) {
        return mappingLookup().isMultiField(field);
    }

    /**
     * Reload any search analyzers that have reloadable components if resource is {@code null},
     * otherwise only the provided resource is reloaded.
     * @param registry the analysis registry
     * @param resource the name of the reloadable resource or {@code null} if all resources should be reloaded.
     * @param preview {@code false} applies analyzer reloading. {@code true} previews the reloading operation, so analyzers are not reloaded
     *               but the results retrieved. This is useful for understanding analyzers usage in the different indices.
     * @return The names of reloaded resources (or resources that would be reloaded if {@code preview} is true).
     * @throws IOException
     */
    public synchronized List<String> reloadSearchAnalyzers(AnalysisRegistry registry, @Nullable String resource, boolean preview)
        throws IOException {
        logger.debug("reloading search analyzers for index [{}]", indexSettings.getIndex().getName());
        // TODO this should bust the cache somehow. Tracked in https://github.com/elastic/elasticsearch/issues/66722
        return indexAnalyzers.reload(registry, indexSettings, resource, preview);
    }

    /**
     * @return Returns all dynamic templates defined in this mapping.
     */
    public DynamicTemplate[] getAllDynamicTemplates() {
        return documentMapper().mapping().getRoot().dynamicTemplates();
    }

    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    public Function<Query, BitSetProducer> getBitSetProducer() {
        return bitSetProducer;
    }

    public MapperMetrics getMapperMetrics() {
        return mapperMetrics;
    }
}
