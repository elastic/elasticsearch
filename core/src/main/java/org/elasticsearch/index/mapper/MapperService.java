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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     */
    public enum MergeReason {
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

    public static final String DEFAULT_MAPPING = "_default_";
    public static final String INDEX_MAPPER_DYNAMIC_SETTING = "index.mapper.dynamic";
    public static final String INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING = "index.mapping.nested_fields.limit";
    public static final boolean INDEX_MAPPER_DYNAMIC_DEFAULT = true;
    private static ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(
            "_uid", "_id", "_type", "_all", "_parent", "_routing", "_index",
            "_size", "_timestamp", "_ttl"
    );

    private static final Function<MappedFieldType, Analyzer> INDEX_ANALYZER_EXTRACTOR = new Function<MappedFieldType, Analyzer>() {
        public Analyzer apply(MappedFieldType fieldType) {
            return fieldType.indexAnalyzer();
        }
    };
    private static final Function<MappedFieldType, Analyzer> SEARCH_ANALYZER_EXTRACTOR = new Function<MappedFieldType, Analyzer>() {
        public Analyzer apply(MappedFieldType fieldType) {
            return fieldType.searchAnalyzer();
        }
    };
    private static final Function<MappedFieldType, Analyzer> SEARCH_QUOTE_ANALYZER_EXTRACTOR = new Function<MappedFieldType, Analyzer>() {
        public Analyzer apply(MappedFieldType fieldType) {
            return fieldType.searchQuoteAnalyzer();
        }
    };

    private final Settings indexSettings;

    private final IndexSettingsService indexSettingsService;

    private final AnalysisService analysisService;

    /**
     * Will create types automatically if they do not exists in the mapping definition yet
     */
    private final boolean dynamic;

    private volatile String defaultMappingSource;
    private volatile String defaultPercolatorMappingSource;

    private volatile Map<String, DocumentMapper> mappers = ImmutableMap.of();

    private volatile FieldTypeLookup fieldTypes;
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    private final List<DocumentTypeListener> typeListeners = new CopyOnWriteArrayList<>();

    private volatile ImmutableMap<String, MappedFieldType> unmappedFieldTypes = ImmutableMap.of();

    private volatile Set<String> parentTypes = ImmutableSet.of();

    final MapperRegistry mapperRegistry;

    @Inject
    public MapperService(Index index, IndexSettingsService indexSettingsService, AnalysisService analysisService,
                         SimilarityLookupService similarityLookupService,
                         ScriptService scriptService, MapperRegistry mapperRegistry) {
        super(index, indexSettingsService.getSettings());
        this.indexSettings = indexSettingsService.getSettings();
        this.indexSettingsService = indexSettingsService;
        this.analysisService = analysisService;
        this.mapperRegistry = mapperRegistry;
        this.fieldTypes = new FieldTypeLookup();
        this.documentParser = new DocumentMapperParser(indexSettings, this, analysisService, similarityLookupService, scriptService, mapperRegistry);
        this.indexAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultIndexAnalyzer(), INDEX_ANALYZER_EXTRACTOR);
        this.searchAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultSearchAnalyzer(), SEARCH_ANALYZER_EXTRACTOR);
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(analysisService.defaultSearchQuoteAnalyzer(), SEARCH_QUOTE_ANALYZER_EXTRACTOR);

        this.dynamic = this.indexSettings.getAsBoolean(INDEX_MAPPER_DYNAMIC_SETTING, INDEX_MAPPER_DYNAMIC_DEFAULT);
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
        if (index.getName().equals(ScriptService.SCRIPT_INDEX)){
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

    public MapperService(Index index, Settings indexSettings, AnalysisService analysisService,
                         SimilarityLookupService similarityLookupService,
                         ScriptService scriptService, MapperRegistry mapperRegistry) {
        this(index, new IndexSettingsService(index, indexSettings), analysisService, similarityLookupService, scriptService, mapperRegistry);
    }

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
        return  new Iterable<DocumentMapper>() {
            @Override
            public Iterator<DocumentMapper> iterator() {
                final Iterator<DocumentMapper> iterator;
                if (includingDefaultMapping) {
                    iterator = mappers.values().iterator();
                } else {
                    iterator = Iterators.filter(mappers.values().iterator(), NOT_A_DEFAULT_DOC_MAPPER);
                }
                return Iterators.unmodifiableIterator(iterator);
            }
        };
    }

    private static final Predicate<DocumentMapper> NOT_A_DEFAULT_DOC_MAPPER = new Predicate<DocumentMapper>() {
        @Override
        public boolean apply(DocumentMapper input) {
            return !DEFAULT_MAPPING.equals(input.type());
        }
    };

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

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason, boolean updateAllTypes) {
        if (DEFAULT_MAPPING.equals(type)) {
            // verify we can parse it
            // NOTE: never apply the default here
            DocumentMapper mapper = documentParser.parse(type, mappingSource);
            // still add it as a document mapper so we have it registered and, for example, persisted back into
            // the cluster meta data if needed, or checked for existence
            synchronized (this) {
                mappers = newMapBuilder(mappers).put(type, mapper).map();
            }
            try {
                defaultMappingSource = mappingSource.string();
            } catch (IOException e) {
                throw new ElasticsearchGenerationException("failed to un-compress", e);
            }
            return mapper;
        } else {
            synchronized (this) {
                final boolean applyDefault =
                        // the default was already applied if we are recovering
                        reason != MergeReason.MAPPING_RECOVERY
                        // only apply the default mapping if we don't have the type yet
                        && mappers.containsKey(type) == false;
                DocumentMapper mergeWith = parse(type, mappingSource, applyDefault);
                return merge(mergeWith, reason, updateAllTypes);
            }
        }
    }

    private synchronized DocumentMapper merge(DocumentMapper mapper, MergeReason reason, boolean updateAllTypes) {
        if (mapper.type().length() == 0) {
            throw new InvalidTypeNameException("mapping type name is empty");
        }
        if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0_beta1) && mapper.type().length() > 255) {
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
        if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0_beta1) && mapper.type().equals(mapper.parentFieldMapper().type())) {
            throw new IllegalArgumentException("The [_parent.type] option can't point to the same type");
        }
        if (typeNameStartsWithIllegalDot(mapper)) {
            if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0_beta1)) {
                throw new IllegalArgumentException("mapping type name [" + mapper.type() + "] must not start with a '.'");
            } else {
                logger.warn("Type [{}] starts with a '.', it is recommended not to start a type name with a '.'", mapper.type());
            }
        }

        // 1. compute the merged DocumentMapper
        DocumentMapper oldMapper = mappers.get(mapper.type());
        DocumentMapper newMapper;
        if (oldMapper != null) {
            newMapper = oldMapper.merge(mapper.mapping(), updateAllTypes);
        } else {
            newMapper = mapper;
        }

        // 2. check basic sanity of the new mapping
        List<ObjectMapper> objectMappers = new ArrayList<>();
        List<FieldMapper> fieldMappers = new ArrayList<>();
        Collections.addAll(fieldMappers, newMapper.mapping().metadataMappers);
        MapperUtils.collect(newMapper.mapping().root(), objectMappers, fieldMappers);
        checkFieldUniqueness(newMapper.type(), objectMappers, fieldMappers);
        checkObjectsCompatibility(newMapper.type(), objectMappers, fieldMappers, updateAllTypes);

        // 3. update lookup data-structures
        // this will in particular make sure that the merged fields are compatible with other types
        FieldTypeLookup fieldTypes = this.fieldTypes.copyAndAddAll(newMapper.type(), fieldMappers, updateAllTypes);

        boolean hasNested = this.hasNested;
        Map<String, ObjectMapper> fullPathObjectMappers = new HashMap<>(this.fullPathObjectMappers);
        for (ObjectMapper objectMapper : objectMappers) {
            fullPathObjectMappers.put(objectMapper.fullPath(), objectMapper);
            if (objectMapper.nested().isNested()) {
                hasNested = true;
            }
        }
        fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);

        if (reason == MergeReason.MAPPING_UPDATE) {
            checkNestedFieldsLimit(fullPathObjectMappers);
        }

        Set<String> parentTypes = this.parentTypes;
        if (oldMapper == null && newMapper.parentFieldMapper().active()) {
            parentTypes = new HashSet<>(parentTypes.size() + 1);
            parentTypes.addAll(this.parentTypes);
            parentTypes.add(mapper.parentFieldMapper().type());
            parentTypes = Collections.unmodifiableSet(parentTypes);
        }

        Map<String, DocumentMapper> mappers = new HashMap<>(this.mappers);
        mappers.put(newMapper.type(), newMapper);
        for (Map.Entry<String, DocumentMapper> entry : mappers.entrySet()) {
            if (entry.getKey().equals(DEFAULT_MAPPING)) {
                continue;
            }
            DocumentMapper m = entry.getValue();
            // apply changes to the field types back
            m = m.updateFieldType(fieldTypes.fullNameToFieldType);
            entry.setValue(m);
        }
        mappers = Collections.unmodifiableMap(mappers);

        // 4. commit the change
        this.mappers = mappers;
        this.fieldTypes = fieldTypes;
        this.hasNested = hasNested;
        this.fullPathObjectMappers = fullPathObjectMappers;
        this.parentTypes = parentTypes;

        // 5. send notifications about the change
        if (oldMapper == null) {
            // means the mapping was created
            for (DocumentTypeListener typeListener : typeListeners) {
                typeListener.beforeCreate(mapper);
            }
        }

        assert assertSerialization(newMapper);
        assert assertMappersShareSameFieldType();

        return newMapper;
    }

    private boolean assertMappersShareSameFieldType() {
        for (DocumentMapper mapper : docMappers(false)) {
            List<FieldMapper> fieldMappers = new ArrayList<>();
            Collections.addAll(fieldMappers, mapper.mapping().metadataMappers);
            MapperUtils.collect(mapper.root(), new ArrayList<ObjectMapper>(), fieldMappers);
            for (FieldMapper fieldMapper : fieldMappers) {
                assert fieldMapper.fieldType() == fieldTypes.get(fieldMapper.name()) : fieldMapper.name();
            }
        }
        return true;
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
        assert Thread.holdsLock(this);

        // first check within mapping
        final Set<String> objectFullNames = new HashSet<>();
        for (ObjectMapper objectMapper : objectMappers) {
            final String fullPath = objectMapper.fullPath();
            if (objectFullNames.add(fullPath) == false) {
                throw new IllegalArgumentException("Object mapper [" + fullPath + "] is defined twice in mapping for type [" + type + "]");
            }
        }

        {
            // Before 3.0 some metadata mappers are also registered under the root object mapper
            // So we avoid false positives by deduplicating mappers
            // given that we check exact equality, this would still catch the case that a mapper
            // is defined under the root object
            Collection<FieldMapper> uniqueFieldMappers = Collections.newSetFromMap(new IdentityHashMap<FieldMapper, Boolean>());
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

        // then check other types
        for (String fieldName : fieldNames) {
            if (fullPathObjectMappers.containsKey(fieldName)) {
                throw new IllegalArgumentException("[" + fieldName + "] is defined as a field in mapping [" + type
                        + "] but this name is already used for an object in other types");
            }
        }

        for (String objectPath : objectFullNames) {
            if (fieldTypes.get(objectPath) != null) {
                throw new IllegalArgumentException("[" + objectPath + "] is defined as an object in mapping [" + type
                        + "] but this name is already used for a field in other types");
            }
        }
    }

    private void checkObjectsCompatibility(String type, Collection<ObjectMapper> objectMappers, Collection<FieldMapper> fieldMappers, boolean updateAllTypes) {
        assert Thread.holdsLock(this);

        for (ObjectMapper newObjectMapper : objectMappers) {
            ObjectMapper existingObjectMapper = fullPathObjectMappers.get(newObjectMapper.fullPath());
            if (existingObjectMapper != null) {
                // simulate a merge and ignore the result, we are just interested
                // in exceptions here
                existingObjectMapper.merge(newObjectMapper, updateAllTypes);
            }
        }
    }

    private void checkNestedFieldsLimit(Map<String, ObjectMapper> fullPathObjectMappers) {
        long allowedNestedFields = indexSettingsService.getSettings().getAsLong(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING, 50L);
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : fullPathObjectMappers.values()) {
            if (objectMapper.nested().isNested()) {
                actualNestedFields++;
            }
        }
        if (allowedNestedFields >= 0 && actualNestedFields > allowedNestedFields) {
            throw new IllegalArgumentException("Limit of nested fields [" + allowedNestedFields + "] in index [" + index().name() + "] has been exceeded");
        }
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource, boolean applyDefault) throws MapperParsingException {
        String defaultMappingSource;
        if (PercolatorService.TYPE_NAME.equals(mappingType)) {
            defaultMappingSource = this.defaultPercolatorMappingSource;
        }  else {
            defaultMappingSource = this.defaultMappingSource;
        }
        return documentParser.parse(mappingType, mappingSource, applyDefault ? defaultMappingSource : null);
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
            throw new TypeMissingException(index, type, "trying to auto create mapping, but dynamic mapping is disabled");
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
            BooleanQuery.Builder typesBool = new BooleanQuery.Builder();
            for (String type : types) {
                DocumentMapper docMapper = documentMapper(type);
                if (docMapper == null) {
                    typesBool.add(new TermQuery(new Term(TypeFieldMapper.NAME, type)), BooleanClause.Occur.SHOULD);
                } else {
                    typesBool.add(docMapper.typeFilter(), BooleanClause.Occur.SHOULD);
                }
            }
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            bool.add(typesBool.build(), Occur.MUST);
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
        final ImmutableMap<String, MappedFieldType> unmappedFieldMappers = this.unmappedFieldTypes;
        MappedFieldType fieldType = unmappedFieldMappers.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext(type);
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?, ?> builder = typeParser.parse("__anonymous_" + type, ImmutableMap.<String, Object>of(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
            fieldType = ((FieldMapper)builder.build(builderContext)).fieldType();

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            this.unmappedFieldTypes = ImmutableMap.<String, MappedFieldType>builder()
                    .putAll(unmappedFieldMappers)
                    .put(type, fieldType)
                    .build();
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
