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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

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

    private final IndexAnalyzers indexAnalyzers;
    private final DocumentMapperParser documentMapperParser;
    private final DocumentParser documentParser;
    private final Version indexVersionCreated;
    private final Analyzer indexAnalyzer;
    private final MapperRegistry mapperRegistry;
    private final Supplier<Mapper.TypeParser.ParserContext> parserContextSupplier;

    /**
     * The current mapping accessed through {@link #snapshot()} and {@link #indexAnalyzer()}.
     */
    private volatile AbstractSnapshot snapshot = new EmptySnapshot(this);

    public MapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers, NamedXContentRegistry xContentRegistry,
                         SimilarityService similarityService, MapperRegistry mapperRegistry,
                         Supplier<QueryShardContext> queryShardContextSupplier, BooleanSupplier idFieldDataEnabled,
                         ScriptService scriptService) {
        super(indexSettings);
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        indexAnalyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                return snapshot.indexAnalyzer();
            }
        };
        this.indexAnalyzers = indexAnalyzers;
        this.mapperRegistry = mapperRegistry;
        Function<DateFormatter, Mapper.TypeParser.ParserContext> parserContextFunction =
            dateFormatter -> new Mapper.TypeParser.ParserContext(similarityService::getSimilarity, mapperRegistry.getMapperParsers()::get,
                mapperRegistry.getRuntimeFieldTypeParsers()::get, indexVersionCreated, queryShardContextSupplier, dateFormatter,
                scriptService, indexAnalyzers, indexSettings, idFieldDataEnabled);
        this.documentParser = new DocumentParser(xContentRegistry, parserContextFunction);
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
            mapperRegistry.getMetadataMapperParsers(indexSettings.getIndexVersionCreated());
        this.parserContextSupplier = () -> parserContextFunction.apply(null);
        this.documentMapperParser = new DocumentMapperParser(indexSettings, indexAnalyzers, this::resolveDocumentType, documentParser,
            this::getMetadataMappers, parserContextSupplier, metadataMapperParsers);
    }

    /**
     * {@code volatile} read of an immutable snapshot of the current mapping.
     */
    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * Does this index contain nested documents?
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#hasNested} on
     *             it as many times as needed.
     */
    @Deprecated
    public boolean hasNested() {
        return snapshot.hasNested();
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public NamedAnalyzer getNamedAnalyzer(String analyzerName) {
        return this.indexAnalyzers.get(analyzerName);
    }

    public Mapper.TypeParser.ParserContext parserContext() {
        return parserContextSupplier.get();
    }

    DocumentParser documentParser() {
        return this.documentParser;
    }

    Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> getMetadataMappers() {
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();
        snapshot.collectMetadataMappers(metadataMappers);
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
     * Update mapping by only merging the metadata that is different between received and stored entries
     */
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected " + index()
            + " but was " + newIndexMetadata.getIndex();

        if (currentIndexMetadata != null && currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
            assertMappingVersion(currentIndexMetadata, newIndexMetadata, snapshot);
            return false;
        }

        final Snapshot updatedSnapshot;
        try {
            updatedSnapshot = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        if (updatedSnapshot == null) {
            return false;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedSnapshot);

        MappingMetadata mappingMetadata = newIndexMetadata.mapping();
        CompressedXContent incomingMappingSource = mappingMetadata.source();

        String op = snapshot != null ? "updated" : "added";
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
        DocumentMapper documentMapper = snapshot.documentMapper();
        if (documentMapper.mappingSource().equals(incomingMappingSource) == false) {
            logger.debug("[{}] parsed mapping, and got different sources\noriginal:\n{}\nparsed:\n{}",
                index(), incomingMappingSource, documentMapper.mappingSource());

            requireRefresh = true;
        }
        return requireRefresh;
    }

    private void assertMappingVersion(
            final IndexMetadata currentIndexMetadata,
            final IndexMetadata newIndexMetadata,
            final Snapshot updatedSnapshot) throws IOException {
        if (Assertions.ENABLED && currentIndexMetadata != null) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedSnapshot == snapshot;

                MappingMetadata mapping = newIndexMetadata.mapping();
                if (mapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.mapping().source();
                    final CompressedXContent newSource = mapping.source();
                    assert currentSource.equals(newSource) :
                        "expected current mapping [" + currentSource + "] for type [" + mapping.type() + "] "
                            + "to be the same as new mapping [" + newSource + "]";
                    final CompressedXContent mapperSource = new CompressedXContent(Strings.toString(snapshot.documentMapper()));
                    assert currentSource.equals(mapperSource) :
                        "expected current mapping [" + currentSource + "] for type [" + mapping.type() + "] "
                            + "to be the same as new mapping [" + mapperSource + "]";
                }

            } else {
                // the mapping version should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetadata.getMappingVersion();
                final long newMappingVersion = newIndexMetadata.getMappingVersion();
                assert currentMappingVersion < newMappingVersion :
                    "expected current mapping version [" + currentMappingVersion + "] "
                        + "to be less than new mapping version [" + newMappingVersion + "]";
                assert updatedSnapshot != null;
                final MappingMetadata currentMapping = currentIndexMetadata.mapping();
                if (currentMapping != null) {
                    final CompressedXContent currentSource = currentMapping.source();
                    final CompressedXContent newSource = updatedSnapshot.documentMapper().mappingSource();
                    assert currentSource.equals(newSource) == false :
                        "expected current mapping [" + currentSource + "] to be different than new mapping [" + newSource + "]";
                }
            }
        }
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        internalMerge(type, content, reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        Snapshot updatedSnapshot = internalMerge(type, mappingSource, reason);
        return updatedSnapshot == null ? null : updatedSnapshot.documentMapper();
    }

    private synchronized Snapshot internalMerge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            return internalMerge(mappingMetadata.type(), mappingMetadata.source(), reason);
        }
        return null;
    }

    private synchronized Snapshot internalMerge(String type, CompressedXContent mappings, MergeReason reason) {

        DocumentMapper documentMapper;

        try {
            documentMapper = documentMapperParser.parse(type, mappings);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }

        return internalMerge(documentMapper, reason);
    }

    private synchronized Snapshot internalMerge(DocumentMapper mapper, MergeReason reason) {

        assert mapper != null;

        // compute the merged DocumentMapper
        MappedSnapshot newSnapshot = this.snapshot.merge(mapper, reason);
        newSnapshot.documentMapper().root().fixRedundantIncludes();
        newSnapshot.documentMapper().validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);

        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return newSnapshot;
        }

        // commit the change
        this.snapshot = newSnapshot;
        assert assertSerialization(newSnapshot);

        return newSnapshot;
    }

    private boolean assertSerialization(MappedSnapshot snapshot) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = snapshot.mapper.mappingSource();
        DocumentMapper newMapper = parse(snapshot.mapper.type(), mappingSource);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException("DocumentMapper serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapper.mappingSource() + "]");
        }
        return true;
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource) throws MapperParsingException {
        return documentMapperParser.parse(mappingType, mappingSource);
    }

    @Deprecated
    public DocumentMapper documentMapper() {
        return snapshot.documentMapper();
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
            Snapshot currentSnapshot = snapshot;
            if (currentSnapshot.documentMapper() != null) {
                return currentSnapshot.documentMapper().type();
            }
        }
        return type;
    }

    /**
     * Returns the document mapper for this MapperService.  If no mapper exists,
     * creates one and returns that.
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#documentMapperWithAutoCreate} on
     *             it as many times as needed.
     */
    @Deprecated
    public DocumentMapperForType documentMapperWithAutoCreate() {
        return snapshot.documentMapperWithAutoCreate();
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#fieldType} on
     *             it as many times as needed.
     */
    @Deprecated
    public MappedFieldType fieldType(String fullName) {
        return snapshot.fieldType(fullName);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#simpleMatchToFullName} on
     *             it as many times as needed.
     */
    @Deprecated
    public Set<String> simpleMatchToFullName(String pattern) {
        return snapshot.simpleMatchToFullName(pattern);
    }

    /**
     * All mapped field types with eager global ordinals.
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#getEagerGlobalOrdinalsFields} on
     *             it as many times as needed.
     */
    @Deprecated
    public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
        return snapshot.getEagerGlobalOrdinalsFields();
    }

    /**
     * Get the named object mapper.
     * @deprecated Get a {@link #snapshot} and call {@link Snapshot#getObjectMapper} on
     *             it as many times as needed.
     */
    @Deprecated
    public ObjectMapper getObjectMapper(String name) {
        return snapshot.getObjectMapper(name);
    }

    /**
     * An analyzer that performs a volatile read on the mapping find correct {@link FieldNameAnalyzer}.
     */
    public Analyzer indexAnalyzer() {
        return indexAnalyzer;
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
        return mapperRegistry.isMetadataField(indexVersionCreated, field);
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

    /**
     * An immutable snapshot of the current mapping.
     */
    public interface Snapshot {
        /**
         * Given the full name of a field, returns its {@link MappedFieldType}.
         */
        MappedFieldType fieldType(String fullName);

        boolean hasNested();

        /**
         * Get the named object mapper
         */
        ObjectMapper getObjectMapper(String name);

        /**
         * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
         * then the fields will be returned with a type prefix.
         */
        Set<String> simpleMatchToFullName(String pattern);

        /**
         * Given a field name, returns its possible paths in the _source. For example,
         * the 'source path' for a multi-field is the path to its parent field.
         */
        Set<String> sourcePath(String fullName);

        /**
         * The document mapper for this MapperService.  If no mapper exists,
         * creates one and returns that.
         */
        DocumentMapperForType documentMapperWithAutoCreate();

        /**
         * All mapped field types with eager global ordinals.
         */
        Iterable<MappedFieldType> getEagerGlobalOrdinalsFields();

        FieldNameAnalyzer indexAnalyzer();

        /**
         * Does the index analyzer for this field have token filters that may produce
         * backwards offsets in term vectors
         */
        boolean containsBrokenAnalysis(String field);

        /**
         * Get the actual mapping.
         * @deprecated Prefer any other method. {@link DocumentMapper} doesn't support
         *             runtime fields and is otherwise tightly coupled to the internals
         *             of mappings.
         */
        @Deprecated
        DocumentMapper documentMapper();

        /**
         * Current version of the of the mapping. Increments if the mapping
         * changes locally. Distinct from
         * {@link IndexMetadata#getMappingVersion()} because it purely
         * considers the local mapping changes.
         */
        long version();

        ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException;

        /**
         * The context used to parse field.
         */
        ParserContext parserContext();

        /**
         * @return Whether a field is a metadata field.
         * this method considers all mapper plugins
         */
        boolean isMetadataField(String field);

        IndexAnalyzers getIndexAnalyzers();
    }

    private abstract static class AbstractSnapshot implements Snapshot {
        protected final MapperService mapperService;

        private AbstractSnapshot(MapperService mapperService) {
            this.mapperService = requireNonNull(mapperService);
        }

        /**
         * The context used to parse field.
         */
        public final ParserContext parserContext() {
            // Safe to plumb through to the Snapshot because it is immutable
            return mapperService.parserContext();
        }

        /**
         * @return Whether a field is a metadata field.
         * this method considers all mapper plugins
         */
        public final boolean isMetadataField(String field) {
            // Safe to plumb through to the Snapshot because it is immutable
            return mapperService.isMetadataField(field);
        }

        public final IndexAnalyzers getIndexAnalyzers() {
            // Safe to plumb through to the Snapshot because it is immutable
            return mapperService.getIndexAnalyzers();
        }

        abstract MappedSnapshot merge(DocumentMapper mapper, MergeReason reason);

        abstract void collectMetadataMappers(Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers);
    }

    private static class EmptySnapshot extends AbstractSnapshot {
        EmptySnapshot(MapperService mapperService) {
            super(mapperService);
        }

        @Override
        public MappedFieldType fieldType(String fullName) {
            if (fullName.equals(TypeFieldType.NAME)) {
                return new TypeFieldType("_doc");
            }
            return null;
        }

        @Override
        public boolean hasNested() {
            return false;
        }

        @Override
        public ObjectMapper getObjectMapper(String name) {
            return null;
        }

        @Override
        public Set<String> simpleMatchToFullName(String pattern) {
            return Regex.isSimpleMatchPattern(pattern) ? emptySet() : singleton(pattern);
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return emptySet();
        }

        @Override
        public DocumentMapperForType documentMapperWithAutoCreate() {
            DocumentMapper auto = mapperService.parse(SINGLE_MAPPING_NAME, null);
            return new DocumentMapperForType(auto, auto.mapping());
        }

        @Override
        public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
            return emptySet();
        }

        @Override
        public FieldNameAnalyzer indexAnalyzer() {
            return null;
        }

        @Override
        public boolean containsBrokenAnalysis(String field) {
            return false;
        }

        @Override
        public DocumentMapper documentMapper() {
            return null;
        }

        @Override
        public long version() {
            return 0;
        }

        @Override
        public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
            return null;
        }

        @Override
        void collectMetadataMappers(Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers) {
            Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperService.mapperRegistry.getMetadataMapperParsers(
                mapperService.indexSettings.getIndexVersionCreated()
            );
            for (MetadataFieldMapper.TypeParser parser : metadataMapperParsers.values()) {
                MetadataFieldMapper metadataFieldMapper = parser.getDefault(parserContext());
                metadataMappers.put(metadataFieldMapper.getClass(), metadataFieldMapper);
            }
        }

        @Override
        MappedSnapshot merge(DocumentMapper mapper, MergeReason reason) {
            return new MappedSnapshot(mapperService, mapper, 1);
        }
    }

    static class MappedSnapshot extends AbstractSnapshot {
        private final DocumentMapper mapper;
        /**
         * Current version of the of the mapping. Increments if the mapping
         * changes locally. Distinct from
         * {@link IndexMetadata#getMappingVersion()} because it purely
         * considers the local mapping changes.
         */
        private final long version;

        MappedSnapshot(MapperService mapperService, DocumentMapper mapper, long version) {
            super(mapperService);
            this.mapper = requireNonNull(mapper);
            this.version = version;
        }

        @Override
        public MappedFieldType fieldType(String fullName) {
            if (fullName.equals(TypeFieldType.NAME)) {
                return new TypeFieldType(mapper.type());
            }
            return mapper.mappers().fieldTypes().get(fullName);
        }

        @Override
        public DocumentMapper documentMapper() {
            return mapper;
        }

        @Override
        public boolean hasNested() {
            return mapper.hasNestedObjects();
        }

        @Override
        public ObjectMapper getObjectMapper(String name) {
            return mapper.mappers().objectMappers().get(name);
        }

        @Override
        public Set<String> simpleMatchToFullName(String pattern) {
            return Regex.isSimpleMatchPattern(pattern) ? mapper.mappers().fieldTypes().simpleMatchToFullName(pattern) : singleton(pattern);
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            return mapper.mappers().fieldTypes().sourcePaths(fullName);
        }

        @Override
        public DocumentMapperForType documentMapperWithAutoCreate() {
            return new DocumentMapperForType(mapper, null);
        }

        @Override
        public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
            return mapper.mappers().fieldTypes().filter(MappedFieldType::eagerGlobalOrdinals);
        }

        @Override
        public FieldNameAnalyzer indexAnalyzer() {
            return mapper.mappers().indexAnalyzer();
        }

        @Override
        public boolean containsBrokenAnalysis(String field) {
            return mapper.mappers().indexAnalyzer().containsBrokenAnalysis(field);
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
            return mapper.parse(source);
        }

        @Override
        protected void collectMetadataMappers(Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers) {
            metadataMappers.putAll(mapper.mapping().metadataMappersMap);            
        }

        @Override
        MappedSnapshot merge(DocumentMapper newMapper, MergeReason reason) {
            DocumentMapper merged = mapper.merge(newMapper.mapping(), reason);
            if (merged.mappingSource().equals(mapper.mappingSource())) {
                return this;
            }
            return new MappedSnapshot(mapperService, merged, version + 1);
        }
    }

    /**
     * A mapping snapshot with the "central" methods that are useful for testing.
     */
    public static class StubSnapshot implements Snapshot {
        private final Function<String, MappedFieldType> lookup;
        private final Supplier<Set<String>> fields;

        public StubSnapshot(Function<String, MappedFieldType> lookup) {
            this.lookup = lookup;
            this.fields = () -> {
                throw new UnsupportedOperationException();
            };
        }

        public StubSnapshot(Map<String, MappedFieldType> lookup) {
            this.lookup = lookup::get;
            this.fields = lookup::keySet;
        }

        @Override
        public MappedFieldType fieldType(String fullName) {
            return lookup.apply(fullName);
        }

        @Override
        public Set<String> simpleMatchToFullName(String pattern) {
            if (Regex.isSimpleMatchPattern(pattern) == false) {
                return singleton(pattern);
            }
            if (Regex.isMatchAllPattern(pattern)) {
                return unmodifiableSet(fields.get());
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNested() {
            return false;
        }

        @Override
        public ObjectMapper getObjectMapper(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> sourcePath(String fullName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocumentMapperForType documentMapperWithAutoCreate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldNameAnalyzer indexAnalyzer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsBrokenAnalysis(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocumentMapper documentMapper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ParserContext parserContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMetadataField(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexAnalyzers getIndexAnalyzers() {
            throw new UnsupportedOperationException();
        }
        
    }
}
