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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class MapperService extends AbstractIndexComponent implements Closeable {

    public static final String DEFAULT_MAPPING = "_default_";
    private static ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(
            "_uid", "_id", "_type", "_all", "_parent", "_routing", "_index",
            "_size", "_timestamp", "_ttl"
    );

    private final AnalysisService analysisService;

    /**
     * Will create types automatically if they do not exists in the mapping definition yet
     */
    private final boolean dynamic;

    private volatile String defaultMappingSource;
    private volatile String defaultPercolatorMappingSource;

    private volatile Map<String, DocumentMapper> mappers = emptyMap();

    // A lock for mappings: modifications (put mapping) need to be performed
    // under the write lock and read operations (document parsing) need to be
    // performed under the read lock
    final ReentrantReadWriteLock mappingLock = new ReentrantReadWriteLock();
    private final ReleasableLock mappingWriteLock = new ReleasableLock(mappingLock.writeLock());

    private volatile FieldTypeLookup fieldTypes;
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    private final List<DocumentTypeListener> typeListeners = new CopyOnWriteArrayList<>();

    private volatile Map<String, MappedFieldType> unmappedFieldTypes = emptyMap();

    private volatile Set<String> parentTypes = emptySet();

    final MapperRegistry mapperRegistry;

    public MapperService(IndexSettings indexSettings, AnalysisService analysisService,
                         SimilarityService similarityService, MapperRegistry mapperRegistry) {
        super(indexSettings);
        this.analysisService = analysisService;
        this.fieldTypes = new FieldTypeLookup();
        this.documentParser = new DocumentMapperParser(indexSettings, this, analysisService, similarityService, mapperRegistry);
        this.indexAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultIndexAnalyzer(), p -> p.indexAnalyzer());
        this.searchAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultSearchAnalyzer(), p -> p.searchAnalyzer());
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultSearchQuoteAnalyzer(), p -> p.searchQuoteAnalyzer());
        this.mapperRegistry = mapperRegistry;

        this.dynamic = this.indexSettings.getSettings().getAsBoolean("index.mapper.dynamic", true);
        defaultPercolatorMappingSource = "{\n" +
            "\"_default_\":{\n" +
                "\"properties\" : {\n" +
                    "\"query\" : {\n" +
                        "\"type\" : \"object\",\n" +
                        "\"enabled\" : false\n" +
                    "}\n" +
                "}\n" +
            "}\n" +
        "}";
        if (index().getName().equals(ScriptService.SCRIPT_INDEX)){
            defaultMappingSource =  "{" +
                "\"_default_\": {" +
                    "\"properties\": {" +
                        "\"script\": { \"enabled\": false }," +
                        "\"template\": { \"enabled\": false }" +
                    "}" +
                "}" +
            "}";
        } else {
            defaultMappingSource = "{\"_default_\":{}}";
        }

        if (logger.isTraceEnabled()) {
            logger.trace("using dynamic[{}], default mapping source[{}], default percolator mapping source[{}]", dynamic, defaultMappingSource, defaultPercolatorMappingSource);
        } else if (logger.isDebugEnabled()) {
            logger.debug("using dynamic[{}]", dynamic);
        }
    }

    @Override
    public void close() {
        for (DocumentMapper documentMapper : mappers.values()) {
            documentMapper.close();
        }
    }

    public boolean hasNested() {
        return this.hasNested;
    }

    /**
     * returns an immutable iterator over current document mappers.
     *
     * @param includingDefaultMapping indicates whether the iterator should contain the {@link #DEFAULT_MAPPING} document mapper.
     *                                As is this not really an active type, you would typically set this to false
     */
    public Iterable<DocumentMapper> docMappers(final boolean includingDefaultMapping) {
        return () -> {
            final Collection<DocumentMapper> documentMappers;
            if (includingDefaultMapping) {
                documentMappers = mappers.values();
            } else {
                documentMappers = mappers.values().stream().filter(mapper -> !DEFAULT_MAPPING.equals(mapper.type())).collect(Collectors.toList());
            }
            return Collections.unmodifiableCollection(documentMappers).iterator();
        };
    }

    public AnalysisService analysisService() {
        return this.analysisService;
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    public void addTypeListener(DocumentTypeListener listener) {
        typeListeners.add(listener);
    }

    public void removeTypeListener(DocumentTypeListener listener) {
        typeListeners.remove(listener);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, boolean applyDefault, boolean updateAllTypes) {
        if (DEFAULT_MAPPING.equals(type)) {
            // verify we can parse it
            DocumentMapper mapper = documentParser.parseCompressed(type, mappingSource);
            // still add it as a document mapper so we have it registered and, for example, persisted back into
            // the cluster meta data if needed, or checked for existence
            try (ReleasableLock lock = mappingWriteLock.acquire()) {
                mappers = newMapBuilder(mappers).put(type, mapper).map();
            }
            try {
                defaultMappingSource = mappingSource.string();
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("failed to un-compress", e);
            }
            return mapper;
        } else {
            return merge(parse(type, mappingSource, applyDefault), updateAllTypes);
        }
    }

    // never expose this to the outside world, we need to reparse the doc mapper so we get fresh
    // instances of field mappers to properly remove existing doc mapper
    private DocumentMapper merge(DocumentMapper mapper, boolean updateAllTypes) {
        try (ReleasableLock lock = mappingWriteLock.acquire()) {
            if (mapper.type().length() == 0) {
                throw new InvalidTypeNameException("mapping type name is empty");
            }
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_2_0_0_beta1) && mapper.type().length() > 255) {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] is too long; limit is length 255 but was [" + mapper.type().length() + "]");
            }
            if (mapper.type().charAt(0) == '_') {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] can't start with '_'");
            }
            if (mapper.type().contains("#")) {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] should not include '#' in it");
            }
            if (mapper.type().contains(",")) {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] should not include ',' in it");
            }
            if (mapper.type().equals(mapper.parentFieldMapper().type())) {
                throw new IllegalArgumentException("The [_parent.type] option can't point to the same type");
            }
            if (typeNameStartsWithIllegalDot(mapper)) {
                if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
                    throw new IllegalArgumentException("mapping type name [" + mapper.type() + "] must not start with a '.'");
                } else {
                    logger.warn("Type [{}] starts with a '.', it is recommended not to start a type name with a '.'", mapper.type());
                }
            }
            // we can add new field/object mappers while the old ones are there
            // since we get new instances of those, and when we remove, we remove
            // by instance equality
            DocumentMapper oldMapper = mappers.get(mapper.type());

            if (oldMapper != null) {
                oldMapper.merge(mapper.mapping(), false, updateAllTypes);
                return oldMapper;
            } else {
                Tuple<Collection<ObjectMapper>, Collection<FieldMapper>> newMappers = checkMappersCompatibility(
                        mapper.type(), mapper.mapping(), updateAllTypes);
                Collection<ObjectMapper> newObjectMappers = newMappers.v1();
                Collection<FieldMapper> newFieldMappers = newMappers.v2();
                addMappers(mapper.type(), newObjectMappers, newFieldMappers);

                for (DocumentTypeListener typeListener : typeListeners) {
                    typeListener.beforeCreate(mapper);
                }
                mappers = newMapBuilder(mappers).put(mapper.type(), mapper).map();
                if (mapper.parentFieldMapper().active()) {
                    Set<String> newParentTypes = new HashSet<>(parentTypes.size() + 1);
                    newParentTypes.addAll(parentTypes);
                    newParentTypes.add(mapper.parentFieldMapper().type());
                    parentTypes = unmodifiableSet(newParentTypes);
                }
                assert assertSerialization(mapper);
                return mapper;
            }
        }
    }

    private boolean typeNameStartsWithIllegalDot(DocumentMapper mapper) {
        return mapper.type().startsWith(".") && !PercolatorService.TYPE_NAME.equals(mapper.type());
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

    private void checkFieldUniqueness(String type, Collection<ObjectMapper> objectMappers, Collection<FieldMapper> fieldMappers) {
        final Set<String> objectFullNames = new HashSet<>();
        for (ObjectMapper objectMapper : objectMappers) {
            final String fullPath = objectMapper.fullPath();
            if (objectFullNames.add(fullPath) == false) {
                throw new IllegalArgumentException("Object mapper [" + fullPath + "] is defined twice in mapping for type [" + type + "]");
            }
        }

        if (indexSettings.getIndexVersionCreated().before(Version.V_3_0_0)) {
            // Before 3.0 some metadata mappers are also registered under the root object mapper
            // So we avoid false positives by deduplicating mappers
            // given that we check exact equality, this would still catch the case that a mapper
            // is defined under the root object 
            Collection<FieldMapper> uniqueFieldMappers = Collections.newSetFromMap(new IdentityHashMap<>());
            uniqueFieldMappers.addAll(fieldMappers);
            fieldMappers = uniqueFieldMappers;
        }

        final Set<String> fieldNames = new HashSet<>();
        for (FieldMapper fieldMapper : fieldMappers) {
            final String name = fieldMapper.name();
            if (objectFullNames.contains(name)) {
                throw new IllegalArgumentException("Field [" + name + "] is defined both as an object and a field in [" + type + "]");
            } else if (fieldNames.add(name) == false) {
                throw new IllegalArgumentException("Field [" + name + "] is defined twice in [" + type + "]");
            }
        }
    }

    protected void checkMappersCompatibility(String type, Collection<ObjectMapper> objectMappers, Collection<FieldMapper> fieldMappers, boolean updateAllTypes) {
        assert mappingLock.isWriteLockedByCurrentThread();

        checkFieldUniqueness(type, objectMappers, fieldMappers);

        for (ObjectMapper newObjectMapper : objectMappers) {
            ObjectMapper existingObjectMapper = fullPathObjectMappers.get(newObjectMapper.fullPath());
            if (existingObjectMapper != null) {
                // simulate a merge and ignore the result, we are just interested
                // in exceptions here
                existingObjectMapper.merge(newObjectMapper, updateAllTypes);
            }
        }

        for (FieldMapper fieldMapper : fieldMappers) {
            if (fullPathObjectMappers.containsKey(fieldMapper.name())) {
                throw new IllegalArgumentException("Field [{}] is defined as a field in mapping [" + fieldMapper.name() + "] but this name is already used for an object in other types");
            }
        }

        fieldTypes.checkCompatibility(type, fieldMappers, updateAllTypes);
    }

    protected Tuple<Collection<ObjectMapper>, Collection<FieldMapper>> checkMappersCompatibility(
            String type, Mapping mapping, boolean updateAllTypes) {
        List<ObjectMapper> objectMappers = new ArrayList<>();
        List<FieldMapper> fieldMappers = new ArrayList<>();
        Collections.addAll(fieldMappers, mapping.metadataMappers);
        MapperUtils.collect(mapping.root, objectMappers, fieldMappers);
        checkMappersCompatibility(type, objectMappers, fieldMappers, updateAllTypes);
        return new Tuple<>(objectMappers, fieldMappers);
    }

    protected void addMappers(String type, Collection<ObjectMapper> objectMappers, Collection<FieldMapper> fieldMappers) {
        assert mappingLock.isWriteLockedByCurrentThread();
        Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>(this.fullPathObjectMappers);
        for (ObjectMapper objectMapper : objectMappers) {
            fullPathObjectMappers.put(objectMapper.fullPath(), objectMapper);
            if (objectMapper.nested().isNested()) {
                hasNested = true;
            }
        }
        this.fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);
        this.fieldTypes = this.fieldTypes.copyAndAddAll(type, fieldMappers);
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource, boolean applyDefault) throws MapperParsingException {
        String defaultMappingSource;
        if (PercolatorService.TYPE_NAME.equals(mappingType)) {
            defaultMappingSource = this.defaultPercolatorMappingSource;
        }  else {
            defaultMappingSource = this.defaultMappingSource;
        }
        return documentParser.parseCompressed(mappingType, mappingSource, applyDefault ? defaultMappingSource : null);
    }

    public boolean hasMapping(String mappingType) {
        return mappers.containsKey(mappingType);
    }

    /**
     * Return the set of concrete types that have a mapping.
     * NOTE: this does not return the default mapping.
     */
    public Collection<String> types() {
        final Set<String> types = new HashSet<>(mappers.keySet());
        types.remove(DEFAULT_MAPPING);
        return Collections.unmodifiableSet(types);
    }

    /**
     * Return the {@link DocumentMapper} for the given type. By using the special
     * {@value #DEFAULT_MAPPING} type, you can get a {@link DocumentMapper} for
     * the default mapping.
     */
    public DocumentMapper documentMapper(String type) {
        return mappers.get(type);
    }

    /**
     * Returns the document mapper created, including a mapping update if the
     * type has been dynamically created.
     */
    public DocumentMapperForType documentMapperWithAutoCreate(String type) {
        DocumentMapper mapper = mappers.get(type);
        if (mapper != null) {
            return new DocumentMapperForType(mapper, null);
        }
        if (!dynamic) {
            throw new TypeMissingException(index(), type, "trying to auto create mapping, but dynamic mapping is disabled");
        }
        mapper = parse(type, null, true);
        return new DocumentMapperForType(mapper, mapper.mapping());
    }

    /**
     * A filter for search. If a filter is required, will return it, otherwise, will return <tt>null</tt>.
     */
    @Nullable
    public Query searchFilter(String... types) {
        boolean filterPercolateType = hasMapping(PercolatorService.TYPE_NAME);
        if (types != null && filterPercolateType) {
            for (String type : types) {
                if (PercolatorService.TYPE_NAME.equals(type)) {
                    filterPercolateType = false;
                    break;
                }
            }
        }
        Query percolatorType = null;
        if (filterPercolateType) {
            percolatorType = documentMapper(PercolatorService.TYPE_NAME).typeFilter();
        }

        if (types == null || types.length == 0) {
            if (hasNested && filterPercolateType) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(Queries.newNonNestedFilter(), Occur.MUST);
                return new ConstantScoreQuery(bq.build());
            } else if (hasNested) {
                return Queries.newNonNestedFilter();
            } else if (filterPercolateType) {
                return new ConstantScoreQuery(Queries.not(percolatorType));
            } else {
                return null;
            }
        }
        // if we filter by types, we don't need to filter by non nested docs
        // since they have different types (starting with __)
        if (types.length == 1) {
            DocumentMapper docMapper = documentMapper(types[0]);
            Query filter = docMapper != null ? docMapper.typeFilter() : new TermQuery(new Term(TypeFieldMapper.NAME, types[0]));
            if (filterPercolateType) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(filter, Occur.MUST);
                return new ConstantScoreQuery(bq.build());
            } else {
                return filter;
            }
        }
        // see if we can use terms filter
        boolean useTermsFilter = true;
        for (String type : types) {
            DocumentMapper docMapper = documentMapper(type);
            if (docMapper == null) {
                useTermsFilter = false;
                break;
            }
            if (docMapper.typeMapper().fieldType().indexOptions() == IndexOptions.NONE) {
                useTermsFilter = false;
                break;
            }
        }

        // We only use terms filter is there is a type filter, this means we don't need to check for hasNested here
        if (useTermsFilter) {
            BytesRef[] typesBytes = new BytesRef[types.length];
            for (int i = 0; i < typesBytes.length; i++) {
                typesBytes[i] = new BytesRef(types[i]);
            }
            TermsQuery termsFilter = new TermsQuery(TypeFieldMapper.NAME, typesBytes);
            if (filterPercolateType) {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(termsFilter, Occur.MUST);
                return new ConstantScoreQuery(bq.build());
            } else {
                return termsFilter;
            }
        } else {
            // Current bool filter requires that at least one should clause matches, even with a must clause.
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            for (String type : types) {
                DocumentMapper docMapper = documentMapper(type);
                if (docMapper == null) {
                    bool.add(new TermQuery(new Term(TypeFieldMapper.NAME, type)), BooleanClause.Occur.SHOULD);
                } else {
                    bool.add(docMapper.typeFilter(), BooleanClause.Occur.SHOULD);
                }
            }
            if (filterPercolateType) {
                bool.add(percolatorType, BooleanClause.Occur.MUST_NOT);
            }
            if (hasNested) {
                bool.add(Queries.newNonNestedFilter(), BooleanClause.Occur.MUST);
            }

            return new ConstantScoreQuery(bool.build());
        }
    }

    /**
     * Returns an {@link MappedFieldType} which has the given index name.
     *
     * If multiple types have fields with the same index name, the first is returned.
     */
    public MappedFieldType indexName(String indexName) {
        return fieldTypes.getByIndexName(indexName);
    }

    /**
     * Returns the {@link MappedFieldType} for the give fullName.
     *
     * If multiple types have fields with the same full name, the first is returned.
     */
    public MappedFieldType fullName(String fullName) {
        return fieldTypes.get(fullName);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     */
    public Collection<String> simpleMatchToIndexNames(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return Collections.singletonList(pattern);
        }
        return fieldTypes.simpleMatchToIndexNames(pattern);
    }

    // TODO: remove this since the underlying index names are now the same across all types
    public Collection<String> simpleMatchToIndexNames(String pattern, @Nullable String[] types) {
        return simpleMatchToIndexNames(pattern);
    }

    // TODO: remove types param, since the object mapper must be the same across all types
    public ObjectMapper getObjectMapper(String name, @Nullable String[] types) {
        return fullPathObjectMappers.get(name);
    }

    public MappedFieldType smartNameFieldType(String smartName) {
        MappedFieldType fieldType = fullName(smartName);
        if (fieldType != null) {
            return fieldType;
        }
        return indexName(smartName);
    }

    // TODO: remove this since the underlying index names are now the same across all types
    public MappedFieldType smartNameFieldType(String smartName, @Nullable String[] types) {
        return smartNameFieldType(smartName);
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public MappedFieldType unmappedFieldType(String type) {
        MappedFieldType fieldType = unmappedFieldTypes.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext(type);
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?, ?> builder = typeParser.parse("__anonymous_" + type, emptyMap(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings.getSettings(), new ContentPath(1));
            fieldType = ((FieldMapper)builder.build(builderContext)).fieldType();

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            Map<String, MappedFieldType> newUnmappedFieldTypes = new HashMap<>();
            newUnmappedFieldTypes.putAll(unmappedFieldTypes);
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
     * Resolves the closest inherited {@link ObjectMapper} that is nested.
     */
    public ObjectMapper resolveClosestNestedObjectMapper(String fieldName) {
        int indexOf = fieldName.lastIndexOf('.');
        if (indexOf == -1) {
            return null;
        } else {
            do {
                String objectPath = fieldName.substring(0, indexOf);
                ObjectMapper objectMapper = fullPathObjectMappers.get(objectPath);
                if (objectMapper == null) {
                    indexOf = objectPath.lastIndexOf('.');
                    continue;
                }

                if (objectMapper.nested().isNested()) {
                    return objectMapper;
                }

                indexOf = objectPath.lastIndexOf('.');
            } while (indexOf != -1);
        }

        return null;
    }

    public Set<String> getParentTypes() {
        return parentTypes;
    }

    /**
     * @return Whether a field is a metadata field.
     */
    public static boolean isMetadataField(String fieldName) {
        return META_FIELDS.contains(fieldName);
    }

    public static String[] getAllMetaFields() {
        return META_FIELDS.toArray(String.class);
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
            MappedFieldType fieldType = smartNameFieldType(fieldName);
            if (fieldType != null) {
                Analyzer analyzer = extractAnalyzer.apply(fieldType);
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return defaultAnalyzer;
        }
    }
}
