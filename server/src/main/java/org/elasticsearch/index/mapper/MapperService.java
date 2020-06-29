/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

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
        MAPPING_RECOVERY;
    }

    public static final String DEFAULT_MAPPING = "_default_";
    public static final String SINGLE_MAPPING_NAME = "_doc";
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
    public static final boolean INDEX_MAPPER_DYNAMIC_DEFAULT = true;
    @Deprecated
    public static final Setting<Boolean> INDEX_MAPPER_DYNAMIC_SETTING =
        Setting.boolSetting("index.mapper.dynamic", INDEX_MAPPER_DYNAMIC_DEFAULT,
            Property.Dynamic, Property.IndexScope, Property.Deprecated);
    // Deprecated set of meta-fields, for checking if a field is meta, use an instance method isMetadataField instead
    @Deprecated
    public static final Set<String> META_FIELDS_BEFORE_7DOT8 =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "_id", IgnoredFieldMapper.NAME, "_index", "_routing", "_size", "_timestamp", "_ttl", "_type")));

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(MapperService.class));
    static final String DEFAULT_MAPPING_ERROR_MESSAGE = "[_default_] mappings are not allowed on new indices and should no " +
        "longer be used. See [https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes-7.0.html" +
        "#default-mapping-not-allowed] for more information.";

    private final IndexAnalyzers indexAnalyzers;

    private volatile String defaultMappingSource;

    private volatile DocumentMapper mapper;
    private volatile DocumentMapper defaultMapper;

    private volatile FieldTypeLookup fieldTypes;
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = emptyMap();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;
    private final Version indexVersionCreated;

    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    private volatile Map<String, MappedFieldType> unmappedFieldTypes = emptyMap();

    final MapperRegistry mapperRegistry;

    private final BooleanSupplier idFieldDataEnabled;

    public MapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers, NamedXContentRegistry xContentRegistry,
                         SimilarityService similarityService, MapperRegistry mapperRegistry,
                         Supplier<QueryShardContext> queryShardContextSupplier, BooleanSupplier idFieldDataEnabled) {
        super(indexSettings);
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.indexAnalyzers = indexAnalyzers;
        this.fieldTypes = new FieldTypeLookup();
        this.documentParser = new DocumentMapperParser(indexSettings, this, xContentRegistry, similarityService, mapperRegistry,
            queryShardContextSupplier);
        this.indexAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultIndexAnalyzer(), p -> p.indexAnalyzer());
        this.searchAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchAnalyzer(), p -> p.searchAnalyzer());
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchQuoteAnalyzer(), p -> p.searchQuoteAnalyzer());
        this.mapperRegistry = mapperRegistry;
        this.idFieldDataEnabled = idFieldDataEnabled;

        if (INDEX_MAPPER_DYNAMIC_SETTING.exists(indexSettings.getSettings()) &&
            indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException("Setting " + INDEX_MAPPER_DYNAMIC_SETTING.getKey() + " was removed after version 6.0.0");
        }

        defaultMappingSource = "{\"_default_\":{}}";

        if (logger.isTraceEnabled()) {
            logger.trace("default mapping source[{}]", defaultMappingSource);
        }
    }

    public boolean hasNested() {
        return this.hasNested;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public NamedAnalyzer getNamedAnalyzer(String analyzerName) {
        return this.indexAnalyzers.get(analyzerName);
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)) {
            return parser.map();
        }
    }

    /**
     * Update mapping by only merging the metadata that is different between received and stored entries
     */
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected " + index()
            + " but was " + newIndexMetadata.getIndex();
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
            // only update entries if needed
            updatedEntries = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY, true);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedEntries);

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
                logger.debug("[{}] {} mapping [{}] (source suppressed due to length, use TRACE level if needed)",
                    index(), op, mappingType);
            }

            // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
            // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
            // merge version of it, which it does when refreshing the mappings), and warn log it.
            if (documentMapper(mappingType).mappingSource().equals(incomingMappingSource) == false) {
                logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}",
                    index(), mappingType, incomingMappingSource, documentMapper(mappingType).mappingSource());

                requireRefresh = true;
            }
        }

        return requireRefresh;
    }

    private void assertMappingVersion(
        final IndexMetadata currentIndexMetadata,
        final IndexMetadata newIndexMetadata,
        final Map<String, DocumentMapper> updatedEntries) {
        if (Assertions.ENABLED
            && currentIndexMetadata != null
            && currentIndexMetadata.getCreationVersion().onOrAfter(Version.V_6_5_0)) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedEntries.isEmpty() : updatedEntries;

                MappingMetadata defaultMapping = newIndexMetadata.defaultMapping();
                if (defaultMapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.defaultMapping().source();
                    final CompressedXContent newSource = defaultMapping.source();
                    assert currentSource.equals(newSource) :
                        "expected current mapping [" + currentSource + "] for type [" + defaultMapping.type() + "] "
                            + "to be the same as new mapping [" + newSource + "]";
                }

                MappingMetadata mapping = newIndexMetadata.mapping();
                if (mapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.mapping().source();
                    final CompressedXContent newSource = mapping.source();
                    assert currentSource.equals(newSource) :
                        "expected current mapping [" + currentSource + "] for type [" + mapping.type() + "] "
                            + "to be the same as new mapping [" + newSource + "]";
                }

            } else {
                // if the mapping version is changed, it should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetadata.getMappingVersion();
                final long newMappingVersion = newIndexMetadata.getMappingVersion();
                assert currentMappingVersion < newMappingVersion :
                    "expected current mapping version [" + currentMappingVersion + "] "
                        + "to be less than new mapping version [" + newMappingVersion + "]";
                assert updatedEntries.isEmpty() == false;
                for (final DocumentMapper documentMapper : updatedEntries.values()) {
                    final MappingMetadata currentMapping;
                    if (documentMapper.type().equals(MapperService.DEFAULT_MAPPING)) {
                        currentMapping = currentIndexMetadata.defaultMapping();
                    } else {
                        currentMapping = currentIndexMetadata.mapping();
                        assert currentMapping == null || documentMapper.type().equals(currentMapping.type());
                    }
                    if (currentMapping != null) {
                        final CompressedXContent currentSource = currentMapping.source();
                        final CompressedXContent newSource = documentMapper.mappingSource();
                        assert currentSource.equals(newSource) == false :
                            "expected current mapping [" + currentSource + "] for type [" + documentMapper.type() + "] " +
                                "to be different than new mapping";
                    }
                }
            }
        }
    }

    public void merge(Map<String, Map<String, Object>> mappings, MergeReason reason) {
        Map<String, CompressedXContent> mappingSourcesCompressed = new LinkedHashMap<>(mappings.size());
        for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
            try {
                mappingSourcesCompressed.put(entry.getKey(), new CompressedXContent(Strings.toString(
                    XContentFactory.jsonBuilder().map(entry.getValue()))));
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        internalMerge(mappingSourcesCompressed, reason);
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        internalMerge(Collections.singletonMap(type, content), reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason, false);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return internalMerge(Collections.singletonMap(type, mappingSource), reason).get(type);
    }

    private synchronized Map<String, DocumentMapper> internalMerge(IndexMetadata indexMetadata,
                                                                   MergeReason reason, boolean onlyUpdateIfNeeded) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        Map<String, CompressedXContent> map = new LinkedHashMap<>();
        for (ObjectCursor<MappingMetadata> cursor : indexMetadata.getMappings().values()) {
            MappingMetadata mappingMetadata = cursor.value;
            if (onlyUpdateIfNeeded) {
                DocumentMapper existingMapper = documentMapper(mappingMetadata.type());
                if (existingMapper == null || mappingMetadata.source().equals(existingMapper.mappingSource()) == false) {
                    map.put(mappingMetadata.type(), mappingMetadata.source());
                }
            } else {
                map.put(mappingMetadata.type(), mappingMetadata.source());
            }
        }
        return internalMerge(map, reason);
    }

    private synchronized Map<String, DocumentMapper> internalMerge(Map<String, CompressedXContent> mappings, MergeReason reason) {
        DocumentMapper defaultMapper = null;
        String defaultMappingSource = null;

        if (mappings.containsKey(DEFAULT_MAPPING)) {
            // verify we can parse it
            // NOTE: never apply the default here
            try {
                defaultMapper = documentParser.parse(DEFAULT_MAPPING, mappings.get(DEFAULT_MAPPING));
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

        DocumentMapper documentMapper = null;
        for (Map.Entry<String, CompressedXContent> entry : mappings.entrySet()) {
            String type = entry.getKey();
            if (type.equals(DEFAULT_MAPPING)) {
                continue;
            }

            if (documentMapper != null) {
                throw new IllegalArgumentException("Cannot put multiple mappings: " + mappings.keySet());
            }

            final boolean applyDefault =
                // the default was already applied if we are recovering
                reason != MergeReason.MAPPING_RECOVERY
                    // only apply the default mapping if we don't have the type yet
                    && this.mapper == null;

            try {
                documentMapper =
                    documentParser.parse(type, entry.getValue(), applyDefault ? defaultMappingSourceOrLastStored : null);
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        return internalMerge(defaultMapper, defaultMappingSource, documentMapper, reason);
    }

    static void validateTypeName(String type) {
        if (type.length() == 0) {
            throw new InvalidTypeNameException("mapping type name is empty");
        }
        if (type.length() > 255) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] is too long; limit is length 255 but was ["
                + type.length() + "]");
        }
        if (type.charAt(0) == '_' && SINGLE_MAPPING_NAME.equals(type) == false) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] can't start with '_' unless it is called ["
                + SINGLE_MAPPING_NAME + "]");
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

    private synchronized Map<String, DocumentMapper> internalMerge(@Nullable DocumentMapper defaultMapper,
                                                                   @Nullable String defaultMappingSource, DocumentMapper mapper,
                                                                   MergeReason reason) {
        boolean hasNested = this.hasNested;
        Map<String, ObjectMapper> fullPathObjectMappers = this.fullPathObjectMappers;

        Map<String, DocumentMapper> results = new LinkedHashMap<>(2);

        if (defaultMapper != null) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
                throw new IllegalArgumentException(DEFAULT_MAPPING_ERROR_MESSAGE);
            } else if (reason == MergeReason.MAPPING_UPDATE) { // only log in case of explicit mapping updates
                deprecationLogger.deprecatedAndMaybeLog("default_mapping_not_allowed", DEFAULT_MAPPING_ERROR_MESSAGE);
            }
            assert defaultMapper.type().equals(DEFAULT_MAPPING);
            results.put(DEFAULT_MAPPING, defaultMapper);
        }

        DocumentMapper newMapper = null;
        FieldTypeLookup newFieldTypes = null;
        if (mapper != null) {
            // check naming
            validateTypeName(mapper.type());

            // compute the merged DocumentMapper
            DocumentMapper oldMapper = this.mapper;
            if (oldMapper != null) {
                newMapper = oldMapper.merge(mapper.mapping(), reason);
            } else {
                newMapper = mapper;
            }

            newMapper.root().fixRedundantIncludes();

            // check basic sanity of the new mapping
            List<ObjectMapper> objectMappers = new ArrayList<>();
            List<FieldMapper> fieldMappers = new ArrayList<>();
            List<FieldAliasMapper> fieldAliasMappers = new ArrayList<>();
            MetadataFieldMapper[] metadataMappers = newMapper.mapping().metadataMappers;
            Collections.addAll(fieldMappers, metadataMappers);
            MapperUtils.collect(newMapper.mapping().root(), objectMappers, fieldMappers, fieldAliasMappers);

            MapperMergeValidator.validateNewMappers(objectMappers, fieldMappers, fieldAliasMappers);
            checkPartitionedIndexConstraints(newMapper);

            // update lookup data-structures
            newFieldTypes = new FieldTypeLookup(fieldMappers, fieldAliasMappers);

            for (ObjectMapper objectMapper : objectMappers) {
                if (fullPathObjectMappers == this.fullPathObjectMappers) {
                    // first time through the loops
                    fullPathObjectMappers = new HashMap<>(this.fullPathObjectMappers);
                }
                fullPathObjectMappers.put(objectMapper.fullPath(), objectMapper);

                if (objectMapper.nested().isNested()) {
                    hasNested = true;
                }
            }

            MapperMergeValidator.validateFieldReferences(fieldMappers, fieldAliasMappers,
                fullPathObjectMappers, newFieldTypes);

            ContextMapping.validateContextPaths(indexSettings.getIndexVersionCreated(), fieldMappers, newFieldTypes::get);

            if (reason != MergeReason.MAPPING_RECOVERY) {
                // this check will only be performed on the master node when there is
                // a call to the update mapping API. For all other cases like
                // the master node restoring mappings from disk or data nodes
                // deserializing cluster state that was sent by the master node,
                // this check will be skipped.
                // Also, don't take metadata mappers into account for the field limit check
                checkTotalFieldsLimit(objectMappers.size() + fieldMappers.size() - metadataMappers.length
                    + fieldAliasMappers.size());
                checkFieldNameSoftLimit(objectMappers, fieldMappers, fieldAliasMappers);
                checkNestedFieldsLimit(fullPathObjectMappers);
                checkDepthLimit(fullPathObjectMappers.keySet());
            }

            results.put(newMapper.type(), newMapper);
        }
        checkIndexSortCompatibility(indexSettings.getIndexSortConfig(), hasNested);

        // make structures immutable
        results = Collections.unmodifiableMap(results);

        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return results;
        }

        // only need to immutably rewrap these if the previous reference was changed.
        // if not then they are already implicitly immutable.
        if (fullPathObjectMappers != this.fullPathObjectMappers) {
            fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);
        }

        // commit the change
        if (defaultMappingSource != null) {
            this.defaultMappingSource = defaultMappingSource;
            this.defaultMapper = defaultMapper;
        }
        if (newMapper != null) {
            this.mapper = newMapper;
            this.fieldTypes = newFieldTypes;
        }
        this.hasNested = hasNested;
        this.fullPathObjectMappers = fullPathObjectMappers;

        assert results.values().stream().allMatch(this::assertSerialization);

        return results;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        DocumentMapper newMapper = parse(mapper.type(), mappingSource, false);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException("DocumentMapper serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapper.mappingSource() + "]");
        }
        return true;
    }

    private void checkNestedFieldsLimit(Map<String, ObjectMapper> fullPathObjectMappers) {
        long allowedNestedFields = indexSettings.getMappingNestedFieldsLimit();
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : fullPathObjectMappers.values()) {
            if (objectMapper.nested().isNested()) {
                actualNestedFields++;
            }
        }
        if (actualNestedFields > allowedNestedFields) {
            throw new IllegalArgumentException("Limit of nested fields [" + allowedNestedFields + "] in index [" + index().getName()
                + "] has been exceeded");
        }
    }

    private void checkTotalFieldsLimit(long totalMappers) {
        long allowedTotalFields = indexSettings.getMappingTotalFieldsLimit();
        if (allowedTotalFields < totalMappers) {
            throw new IllegalArgumentException("Limit of total fields [" + allowedTotalFields + "] in index [" + index().getName()
                + "] has been exceeded");
        }
    }

    private void checkDepthLimit(Collection<String> objectPaths) {
        final long maxDepth = indexSettings.getMappingDepthLimit();
        for (String objectPath : objectPaths) {
            checkDepthLimit(objectPath, maxDepth);
        }
    }

    private void checkDepthLimit(String objectPath, long maxDepth) {
        int numDots = 0;
        for (int i = 0; i < objectPath.length(); ++i) {
            if (objectPath.charAt(i) == '.') {
                numDots += 1;
            }
        }
        final int depth = numDots + 2;
        if (depth > maxDepth) {
            throw new IllegalArgumentException("Limit of mapping depth [" + maxDepth + "] in index [" + index().getName()
                + "] has been exceeded due to object field [" + objectPath + "]");
        }
    }

    private void checkFieldNameSoftLimit(Collection<ObjectMapper> objectMappers,
                                         Collection<FieldMapper> fieldMappers,
                                         Collection<FieldAliasMapper> fieldAliasMappers) {
        final long maxFieldNameLength = indexSettings.getMappingFieldNameLengthLimit();

        Stream.of(objectMappers.stream(), fieldMappers.stream(), fieldAliasMappers.stream())
            .reduce(Stream::concat)
            .orElseGet(Stream::empty)
            .forEach(mapper -> {
                String name = mapper.simpleName();
                if (name.length() > maxFieldNameLength) {
                    throw new IllegalArgumentException("Field name [" + name + "] in index [" + index().getName() +
                        "] is too long. The limit is set to [" + maxFieldNameLength + "] characters but was ["
                        + name.length() + "] characters");
                }
            });
    }

    private void checkPartitionedIndexConstraints(DocumentMapper newMapper) {
        if (indexSettings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (!newMapper.routingFieldMapper().required()) {
                throw new IllegalArgumentException("mapping type [" + newMapper.type() + "] must have routing "
                    + "required for partitioned index [" + indexSettings.getIndex().getName() + "]");
            }
        }
    }

    private static void checkIndexSortCompatibility(IndexSortConfig sortConfig, boolean hasNested) {
        if (sortConfig.hasIndexSort() && hasNested) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
        }
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource, boolean applyDefault) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource, applyDefault ? defaultMappingSource : null);
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet.
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
     * Returns the document mapper created, including a mapping update if the
     * type has been dynamically created.
     */
    public DocumentMapperForType documentMapperWithAutoCreate(String type) {
        DocumentMapper mapper = documentMapper(type);
        if (mapper != null) {
            return new DocumentMapperForType(mapper, null);
        }
        mapper = parse(type, null, true);
        return new DocumentMapperForType(mapper, mapper.mapping());
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public MappedFieldType fieldType(String fullName) {
        return fieldTypes.get(fullName);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return Collections.singleton(pattern);
        }
        return fieldTypes.simpleMatchToFullName(pattern);
    }

    /**
     * Returns all mapped field types.
     */
    public Iterable<MappedFieldType> fieldTypes() {
        return fieldTypes;
    }

    public ObjectMapper getObjectMapper(String name) {
        return fullPathObjectMappers.get(name);
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public MappedFieldType unmappedFieldType(String type) {
        if (type.equals("string")) {
            deprecationLogger.deprecatedAndMaybeLog("unmapped_type_string",
                "[unmapped_type:string] should be replaced with [unmapped_type:keyword]");
            type = "keyword";
        }
        MappedFieldType fieldType = unmappedFieldTypes.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?> builder = typeParser.parse("__anonymous_" + type, emptyMap(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings.getSettings(), new ContentPath(1));
            fieldType = ((FieldMapper) builder.build(builderContext)).fieldType();

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            Map<String, MappedFieldType> newUnmappedFieldTypes = new HashMap<>(unmappedFieldTypes);
            newUnmappedFieldTypes.put(type, fieldType);
            unmappedFieldTypes = unmodifiableMap(newUnmappedFieldTypes);
        }
        return fieldType;
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuoteAnalyzer;
    }

    /**
     * Returns <code>true</code> if fielddata is enabled for the {@link IdFieldMapper} field, <code>false</code> otherwise.
     */
    public boolean isIdFieldDataEnabled() {
        return idFieldDataEnabled.getAsBoolean();
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
        return mapperRegistry.isMetadataField(indexVersionCreated, field);
    }

    /**
     * An analyzer wrapper that can lookup fields within the index mappings
     */
    final class MapperAnalyzerWrapper extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;
        private final Function<MappedFieldType, Analyzer> extractAnalyzer;

        MapperAnalyzerWrapper(Analyzer defaultAnalyzer, Function<MappedFieldType, Analyzer> extractAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
            this.extractAnalyzer = extractAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            MappedFieldType fieldType = fieldType(fieldName);
            if (fieldType != null) {
                Analyzer analyzer = extractAnalyzer.apply(fieldType);
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return defaultAnalyzer;
        }
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
        return reloadedAnalyzers;
    }

}
