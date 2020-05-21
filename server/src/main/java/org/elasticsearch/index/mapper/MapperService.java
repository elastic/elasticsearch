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

import com.carrotsearch.hppc.ObjectHashSet;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
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
         * Recovery of an existing mapping, for instance because of a restart,
         * if a shard was moved to a different node or for administrative
         * purposes.
         */
        MAPPING_RECOVERY;
    }

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

    //TODO this needs to be cleaned up: _timestamp and _ttl are not supported anymore, _field_names, _seq_no, _version and _source are
    //also missing, not sure if on purpose. See IndicesModule#getMetadataMappers
    private static final String[] SORTED_META_FIELDS = new String[]{
        "_id", IgnoredFieldMapper.NAME, "_index", "_nested_path", "_routing", "_size", "_timestamp", "_ttl", "_type"
    };

    private static final ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(SORTED_META_FIELDS);

    private final IndexAnalyzers indexAnalyzers;

    private volatile DocumentMapper mapper;

    private volatile FieldTypeLookup fieldTypes;
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = emptyMap();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

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
        this.indexAnalyzers = indexAnalyzers;
        this.fieldTypes = new FieldTypeLookup();
        this.documentParser = new DocumentMapperParser(indexSettings, this, xContentRegistry, similarityService, mapperRegistry,
                queryShardContextSupplier);
        this.indexAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultIndexAnalyzer(), p -> p.indexAnalyzer());
        this.searchAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchAnalyzer(), p -> p.searchAnalyzer());
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchQuoteAnalyzer(), p -> p.searchQuoteAnalyzer());
        this.mapperRegistry = mapperRegistry;
        this.idFieldDataEnabled = idFieldDataEnabled;
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

        final DocumentMapper updatedMapper;
        try {
            // only update entries if needed
            updatedMapper = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY, true);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        if (updatedMapper == null) {
            return false;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedMapper);

        MappingMetadata mappingMetadata = newIndexMetadata.mapping();
        CompressedXContent incomingMappingSource = mappingMetadata.source();

        String op = mapper != null ? "updated" : "added";
        if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
            logger.debug("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else if (logger.isTraceEnabled()) {
            logger.trace("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else {
            logger.debug("[{}] {} mapping (source suppressed due to length, use TRACE level if needed)",
                index(), op);
        }

        // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
        // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
        // merge version of it, which it does when refreshing the mappings), and warn log it.
        if (documentMapper().mappingSource().equals(incomingMappingSource) == false) {
            logger.debug("[{}] parsed mapping, and got different sources\noriginal:\n{}\nparsed:\n{}",
                index(), incomingMappingSource, documentMapper().mappingSource());

            requireRefresh = true;
        }


        return requireRefresh;
    }

    private void assertMappingVersion(
            final IndexMetadata currentIndexMetadata,
            final IndexMetadata newIndexMetadata,
            final DocumentMapper updatedMapper) {
        if (Assertions.ENABLED && currentIndexMetadata != null) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedMapper == mapper;

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
                assert updatedMapper != null;
                final MappingMetadata currentMapping = currentIndexMetadata.mapping();
                if (currentMapping != null) {
                    final CompressedXContent currentSource = currentMapping.source();
                    final CompressedXContent newSource = updatedMapper.mappingSource();
                    assert currentSource.equals(newSource) == false :
                        "expected current mapping [" + currentSource + "] to be different than new mapping";
                }
            }
        }
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        internalMerge(type, content, reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason, false);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return internalMerge(type, mappingSource, reason);
    }

    private synchronized DocumentMapper internalMerge(IndexMetadata indexMetadata,
                                                                   MergeReason reason, boolean onlyUpdateIfNeeded) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            if (onlyUpdateIfNeeded) {
                DocumentMapper existingMapper = documentMapper();
                if (existingMapper == null || mappingMetadata.source().equals(existingMapper.mappingSource()) == false) {
                    return internalMerge(mappingMetadata.type(), mappingMetadata.source(), reason);
                }
            } else {
                return internalMerge(mappingMetadata.type(), mappingMetadata.source(), reason);
            }
        }
        return null;
    }

    private synchronized DocumentMapper internalMerge(String type, CompressedXContent mappings, MergeReason reason) {

        DocumentMapper documentMapper;

        try {
            documentMapper = documentParser.parse(type, mappings);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }

        return internalMerge(documentMapper, reason);
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

    private synchronized DocumentMapper internalMerge(DocumentMapper mapper, MergeReason reason) {
        boolean hasNested = this.hasNested;
        Map<String, ObjectMapper> fullPathObjectMappers = this.fullPathObjectMappers;
        FieldTypeLookup fieldTypes = this.fieldTypes;

        assert mapper != null;
        // check naming
        validateTypeName(mapper.type());

        // compute the merged DocumentMapper
        DocumentMapper oldMapper = this.mapper;
        DocumentMapper newMapper;
        if (oldMapper != null) {
            newMapper = oldMapper.merge(mapper.mapping());
        } else {
            newMapper = mapper;
        }

        // check basic sanity of the new mapping
        List<ObjectMapper> objectMappers = new ArrayList<>();
        List<FieldMapper> fieldMappers = new ArrayList<>();
        List<FieldAliasMapper> fieldAliasMappers = new ArrayList<>();
        MetadataFieldMapper[] metadataMappers = newMapper.mapping().metadataMappers;
        Collections.addAll(fieldMappers, metadataMappers);
        MapperUtils.collect(newMapper.mapping().root(), objectMappers, fieldMappers, fieldAliasMappers);

        MapperMergeValidator.validateNewMappers(objectMappers, fieldMappers, fieldAliasMappers, fieldTypes);
        checkPartitionedIndexConstraints(newMapper);

        // update lookup data-structures
        fieldTypes = fieldTypes.copyAndAddAll(fieldMappers, fieldAliasMappers);

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
            fullPathObjectMappers, fieldTypes);

        ContextMapping.validateContextPaths(indexSettings.getIndexVersionCreated(), fieldMappers, fieldTypes::get);

        if (reason == MergeReason.MAPPING_UPDATE || reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            // this check will only be performed on the master node when there is
            // a call to the update mapping API. For all other cases like
            // the master node restoring mappings from disk or data nodes
            // deserializing cluster state that was sent by the master node,
            // this check will be skipped.
            // Also, don't take metadata mappers into account for the field limit check
            checkTotalFieldsLimit(objectMappers.size() + fieldMappers.size() - metadataMappers.length
                + fieldAliasMappers.size() );
            checkFieldNameSoftLimit(objectMappers, fieldMappers, fieldAliasMappers);
        }

        if (reason == MergeReason.MAPPING_UPDATE || reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            // this check will only be performed on the master node when there is
            // a call to the update mapping API. For all other cases like
            // the master node restoring mappings from disk or data nodes
            // deserializing cluster state that was sent by the master node,
            // this check will be skipped.
            checkNestedFieldsLimit(fullPathObjectMappers);
            checkDepthLimit(fullPathObjectMappers.keySet());
        }
        checkIndexSortCompatibility(indexSettings.getIndexSortConfig(), hasNested);

        if (newMapper != null) {
            DocumentMapper updatedDocumentMapper = newMapper.updateFieldType(fieldTypes.fullNameToFieldType);
            if (updatedDocumentMapper != newMapper) {
                newMapper = updatedDocumentMapper;
            }
        }

        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return newMapper;
        }

        // only need to immutably rewrap these if the previous reference was changed.
        // if not then they are already implicitly immutable.
        if (fullPathObjectMappers != this.fullPathObjectMappers) {
            fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);
        }

        // commit the change
        if (newMapper != null) {
            this.mapper = newMapper;
        }
        this.fieldTypes = fieldTypes;
        this.hasNested = hasNested;
        this.fullPathObjectMappers = fullPathObjectMappers;

        assert assertMappersShareSameFieldType();
        assert newMapper == null || assertSerialization(newMapper);

        return newMapper;
    }

    private boolean assertMappersShareSameFieldType() {
        if (mapper != null) {
            List<FieldMapper> fieldMappers = new ArrayList<>();
            Collections.addAll(fieldMappers, mapper.mapping().metadataMappers);
            MapperUtils.collect(mapper.root(), new ArrayList<>(), fieldMappers, new ArrayList<>());
            for (FieldMapper fieldMapper : fieldMappers) {
                assert fieldMapper.fieldType() == fieldTypes.get(fieldMapper.name()) : fieldMapper.name();
            }
        }
        return true;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        DocumentMapper newMapper = parse(mapper.type(), mappingSource);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException("DocumentMapper serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapper.mappingSource() + "]");
        }
        return true;
    }

    private void checkNestedFieldsLimit(Map<String, ObjectMapper> fullPathObjectMappers) {
        long allowedNestedFields = indexSettings.getValue(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING);
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
        long allowedTotalFields = indexSettings.getValue(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        if (allowedTotalFields < totalMappers) {
            throw new IllegalArgumentException("Limit of total fields [" + allowedTotalFields + "] in index [" + index().getName()
                + "] has been exceeded");
        }
    }

    private void checkDepthLimit(Collection<String> objectPaths) {
        final long maxDepth = indexSettings.getValue(INDEX_MAPPING_DEPTH_LIMIT_SETTING);
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
        final long maxFieldNameLength = indexSettings.getValue(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING);

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

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource);
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet.
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
    public String resolveDocumentType(String type) {
        if (MapperService.SINGLE_MAPPING_NAME.equals(type)) {
            if (mapper != null) {
                return mapper.type();
            }
        }
        return type;
    }

    /**
     * Returns the document mapper for this MapperService.  If no mapper exists,
     * creates one and returns that.
     */
    public DocumentMapperForType documentMapperWithAutoCreate() {
        DocumentMapper mapper = documentMapper();
        if (mapper != null) {
            return new DocumentMapperForType(mapper, null);
        }
        mapper = parse(SINGLE_MAPPING_NAME, null);
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
        MappedFieldType fieldType = unmappedFieldTypes.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?> builder = typeParser.parse("__anonymous_" + type, emptyMap(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings.getSettings(), new ContentPath(1));
            fieldType = ((FieldMapper)builder.build(builderContext)).fieldType();

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
     */
    public static boolean isMetadataField(String fieldName) {
        return META_FIELDS.contains(fieldName);
    }

    public static String[] getAllMetaFields() {
        return Arrays.copyOf(SORTED_META_FIELDS, SORTED_META_FIELDS.length);
    }

    /** An analyzer wrapper that can lookup fields within the index mappings */
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
