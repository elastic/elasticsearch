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
import com.google.common.base.Predicate;
import com.google.common.collect.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class MapperService extends AbstractIndexComponent  {

    public static final String DEFAULT_MAPPING = "_default_";
    private static ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(
            "_uid", "_id", "_type", "_all", "_parent", "_routing", "_index",
            "_size", "_timestamp", "_ttl"
    );
    private final AnalysisService analysisService;
    private final IndexFieldDataService fieldDataService;

    /**
     * Will create types automatically if they do not exists in the mapping definition yet
     */
    private final boolean dynamic;

    private volatile String defaultMappingSource;
    private volatile String defaultPercolatorMappingSource;

    private volatile Map<String, DocumentMapper> mappers = ImmutableMap.of();

    // A lock for mappings: modifications (put mapping) need to be performed
    // under the write lock and read operations (document parsing) need to be
    // performed under the read lock
    final ReentrantReadWriteLock mappingLock = new ReentrantReadWriteLock();
    private final ReleasableLock mappingWriteLock = new ReleasableLock(mappingLock.writeLock());

    private volatile FieldMappersLookup fieldMappers;
    private volatile ImmutableOpenMap<String, ObjectMappers> fullPathObjectMappers = ImmutableOpenMap.of();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

    private final SmartIndexNameSearchAnalyzer searchAnalyzer;
    private final SmartIndexNameSearchQuoteAnalyzer searchQuoteAnalyzer;

    private final List<DocumentTypeListener> typeListeners = new CopyOnWriteArrayList<>();

    private volatile ImmutableMap<String, FieldMapper> unmappedFieldMappers = ImmutableMap.of();

    private volatile ImmutableSet<String> parentTypes = ImmutableSet.of();

    @Inject
    public MapperService(Index index, @IndexSettings Settings indexSettings, AnalysisService analysisService, IndexFieldDataService fieldDataService,
                         SimilarityLookupService similarityLookupService,
                         ScriptService scriptService) {
        super(index, indexSettings);
        this.analysisService = analysisService;
        this.fieldDataService = fieldDataService;
        this.fieldMappers = new FieldMappersLookup();
        this.documentParser = new DocumentMapperParser(index, indexSettings, this, analysisService, similarityLookupService, scriptService);
        this.searchAnalyzer = new SmartIndexNameSearchAnalyzer(analysisService.defaultSearchAnalyzer());
        this.searchQuoteAnalyzer = new SmartIndexNameSearchQuoteAnalyzer(analysisService.defaultSearchQuoteAnalyzer());

        this.dynamic = indexSettings.getAsBoolean("index.mapper.dynamic", true);
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

    public DocumentMapper merge(String type, CompressedXContent mappingSource, boolean applyDefault) {
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
            return merge(parse(type, mappingSource, applyDefault));
        }
    }

    // never expose this to the outside world, we need to reparse the doc mapper so we get fresh
    // instances of field mappers to properly remove existing doc mapper
    private DocumentMapper merge(DocumentMapper mapper) {
        try (ReleasableLock lock = mappingWriteLock.acquire()) {
            if (mapper.type().length() == 0) {
                throw new InvalidTypeNameException("mapping type name is empty");
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
            if (Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0) && mapper.type().equals(mapper.parentFieldMapper().type())) {
                throw new IllegalArgumentException("The [_parent.type] option can't point to the same type");
            }
            if (mapper.type().contains(".") && !PercolatorService.TYPE_NAME.equals(mapper.type())) {
                logger.warn("Type [{}] contains a '.', it is recommended not to include it within a type name", mapper.type());
            }
            // we can add new field/object mappers while the old ones are there
            // since we get new instances of those, and when we remove, we remove
            // by instance equality
            DocumentMapper oldMapper = mappers.get(mapper.type());

            if (oldMapper != null) {
                MergeResult result = oldMapper.merge(mapper.mapping(), false);
                if (result.hasConflicts()) {
                    // TODO: What should we do???
                    if (logger.isDebugEnabled()) {
                        logger.debug("merging mapping for type [{}] resulted in conflicts: [{}]", mapper.type(), Arrays.toString(result.buildConflicts()));
                    }
                }
                fieldDataService.onMappingUpdate();
                assert assertSerialization(oldMapper);
                return oldMapper;
            } else {
                List<ObjectMapper> newObjectMappers = new ArrayList<>();
                List<FieldMapper> newFieldMappers = new ArrayList<>();
                for (RootMapper rootMapper : mapper.mapping().rootMappers) {
                    if (rootMapper instanceof FieldMapper) {
                        newFieldMappers.add((FieldMapper)rootMapper);
                    }
                }
                MapperUtils.collect(mapper.mapping().root, newObjectMappers, newFieldMappers);
                addFieldMappers(newFieldMappers);
                addObjectMappers(newObjectMappers);

                for (DocumentTypeListener typeListener : typeListeners) {
                    typeListener.beforeCreate(mapper);
                }
                mappers = newMapBuilder(mappers).put(mapper.type(), mapper).map();
                if (mapper.parentFieldMapper().active()) {
                    ImmutableSet.Builder<String> parentTypesCopy = ImmutableSet.builder();
                    parentTypesCopy.addAll(parentTypes);
                    parentTypesCopy.add(mapper.parentFieldMapper().type());
                    parentTypes = parentTypesCopy.build();
                }
                assert assertSerialization(mapper);
                return mapper;
            }
        }
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

    protected void addObjectMappers(Collection<ObjectMapper> objectMappers) {
        assert mappingLock.isWriteLockedByCurrentThread();
        ImmutableOpenMap.Builder<String, ObjectMappers> fullPathObjectMappers = ImmutableOpenMap.builder(this.fullPathObjectMappers);
        for (ObjectMapper objectMapper : objectMappers) {
            ObjectMappers mappers = fullPathObjectMappers.get(objectMapper.fullPath());
            if (mappers == null) {
                mappers = new ObjectMappers(objectMapper);
            } else {
                mappers = mappers.concat(objectMapper);
            }
            fullPathObjectMappers.put(objectMapper.fullPath(), mappers);
            // update the hasNested flag
            if (objectMapper.nested().isNested()) {
                hasNested = true;
            }
        }
        this.fullPathObjectMappers = fullPathObjectMappers.build();
    }

    protected void addFieldMappers(Collection<FieldMapper> fieldMappers) {
        assert mappingLock.isWriteLockedByCurrentThread();
        this.fieldMappers = this.fieldMappers.copyAndAddAll(fieldMappers);
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

    public Collection<String> types() {
        return mappers.keySet();
    }

    public DocumentMapper documentMapper(String type) {
        return mappers.get(type);
    }

    /**
     * Returns the document mapper created, including a mapping update if the
     * type has been dynamically created.
     */
    public Tuple<DocumentMapper, Mapping> documentMapperWithAutoCreate(String type) {
        DocumentMapper mapper = mappers.get(type);
        if (mapper != null) {
            return Tuple.tuple(mapper, null);
        }
        if (!dynamic) {
            throw new TypeMissingException(index, type, "trying to auto create mapping, but dynamic mapping is disabled");
        }
        mapper = parse(type, null, true);
        return Tuple.tuple(mapper, mapper.mapping());
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
                BooleanQuery bq = new BooleanQuery();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(Queries.newNonNestedFilter(), Occur.MUST);
                return new ConstantScoreQuery(bq);
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
                BooleanQuery bq = new BooleanQuery();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(filter, Occur.MUST);
                return new ConstantScoreQuery(bq);
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
                BooleanQuery bq = new BooleanQuery();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(termsFilter, Occur.MUST);
                return new ConstantScoreQuery(bq);
            } else {
                return termsFilter;
            }
        } else {
            // Current bool filter requires that at least one should clause matches, even with a must clause.
            BooleanQuery bool = new BooleanQuery();
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

            return new ConstantScoreQuery(bool);
        }
    }

    /**
     * Returns an {@link FieldMapper} which has the given index name.
     *
     * If multiple types have fields with the same index name, the first is returned.
     */
    public FieldMapper indexName(String indexName) {
        FieldMappers mappers = fieldMappers.indexName(indexName);
        if (mappers == null) {
            return null;
        }
        return mappers.mapper();
    }

    /**
     * Returns the {@link FieldMappers} of all the {@link FieldMapper}s that are
     * registered under the give fullName across all the different {@link DocumentMapper} types.
     *
     * @param fullName The full name
     * @return All teh {@link FieldMappers} across all the {@link DocumentMapper}s for the given fullName.
     */
    public FieldMapper fullName(String fullName) {
        FieldMappers mappers = fieldMappers.fullName(fullName);
        if (mappers == null) {
            return null;
        }
        return mappers.mapper();
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     */
    public Collection<String> simpleMatchToIndexNames(String pattern) {
        return simpleMatchToIndexNames(pattern, null);
    }
    /**
     * Returns all the fields that match the given pattern, with an optional narrowing
     * based on a list of types.
     */
    public Collection<String> simpleMatchToIndexNames(String pattern, @Nullable String[] types) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return ImmutableList.of(pattern);
        }
        
        if (MetaData.isAllTypes(types)) {
            return fieldMappers.simpleMatchToIndexNames(pattern);
        }

        List<String> fields = Lists.newArrayList();
        for (String type : types) {
            DocumentMapper possibleDocMapper = mappers.get(type);
            if (possibleDocMapper != null) {
                for (String indexName : possibleDocMapper.mappers().simpleMatchToIndexNames(pattern)) {
                    fields.add(indexName);
                }
            }
        }
        return fields;
    }

    public SmartNameObjectMapper smartNameObjectMapper(String smartName, @Nullable String[] types) {
        if (types == null || types.length == 0 || types.length == 1 && types[0].equals("_all")) {
            ObjectMappers mappers = fullPathObjectMappers.get(smartName);
            if (mappers != null) {
                return new SmartNameObjectMapper(mappers.mapper(), guessDocMapper(smartName));
            }
            return null;
        }
        for (String type : types) {
            DocumentMapper possibleDocMapper = mappers.get(type);
            if (possibleDocMapper != null) {
                ObjectMapper mapper = possibleDocMapper.objectMappers().get(smartName);
                if (mapper != null) {
                    return new SmartNameObjectMapper(mapper, possibleDocMapper);
                }
            }
        }
        return null;
    }

    private DocumentMapper guessDocMapper(String path) {
        for (DocumentMapper documentMapper : docMappers(false)) {
            if (documentMapper.objectMappers().containsKey(path)) {
                return documentMapper;
            }
        }
        return null;
    }

    public FieldMapper smartNameFieldMapper(String smartName) {
        FieldMapper mapper = fullName(smartName);
        if (mapper != null) {
            return mapper;
        }
        return indexName(smartName);
    }

    public FieldMapper smartNameFieldMapper(String smartName, @Nullable String[] types) {
        if (types == null || types.length == 0 || types.length == 1 && types[0].equals("_all")) {
            return smartNameFieldMapper(smartName);
        }
        for (String type : types) {
            DocumentMapper documentMapper = mappers.get(type);
            // we found a mapper
            if (documentMapper != null) {
                // see if we find a field for it
                FieldMappers mappers = documentMapper.mappers().smartName(smartName);
                if (mappers != null) {
                    return mappers.mapper();
                }
            }
        }
        return null;
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public FieldMapper unmappedFieldMapper(String type) {
        final ImmutableMap<String, FieldMapper> unmappedFieldMappers = this.unmappedFieldMappers;
        FieldMapper mapper = unmappedFieldMappers.get(type);
        if (mapper == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?, ?> builder = typeParser.parse("__anonymous_" + type, ImmutableMap.<String, Object>of(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
            mapper = (FieldMapper) builder.build(builderContext);

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            this.unmappedFieldMappers = ImmutableMap.<String, FieldMapper>builder()
                    .putAll(unmappedFieldMappers)
                    .put(type, mapper)
                    .build();
        }
        return mapper;
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
                ObjectMappers objectMappers = fullPathObjectMappers.get(objectPath);
                if (objectMappers == null) {
                    indexOf = objectPath.lastIndexOf('.');
                    continue;
                }

                if (objectMappers.hasNested()) {
                    for (ObjectMapper objectMapper : objectMappers) {
                        if (objectMapper.nested().isNested()) {
                            return objectMapper;
                        }
                    }
                }

                indexOf = objectPath.lastIndexOf('.');
            } while (indexOf != -1);
        }

        return null;
    }

    public ImmutableSet<String> getParentTypes() {
        return parentTypes;
    }

    /**
     * @return Whether a field is a metadata field.
     */
    public static boolean isMetadataField(String fieldName) {
        return META_FIELDS.contains(fieldName);
    }

    public static class SmartNameObjectMapper {
        private final ObjectMapper mapper;
        private final DocumentMapper docMapper;

        public SmartNameObjectMapper(ObjectMapper mapper, @Nullable DocumentMapper docMapper) {
            this.mapper = mapper;
            this.docMapper = docMapper;
        }

        public boolean hasMapper() {
            return mapper != null;
        }

        public ObjectMapper mapper() {
            return mapper;
        }

        public boolean hasDocMapper() {
            return docMapper != null;
        }

        public DocumentMapper docMapper() {
            return docMapper;
        }
    }

    final class SmartIndexNameSearchAnalyzer extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;

        SmartIndexNameSearchAnalyzer(Analyzer defaultAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            FieldMapper mapper = smartNameFieldMapper(fieldName);
            if (mapper != null && mapper.fieldType().searchAnalyzer() != null) {
                return mapper.fieldType().searchAnalyzer();
            }
            return defaultAnalyzer;
        }
    }

    final class SmartIndexNameSearchQuoteAnalyzer extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;

        SmartIndexNameSearchQuoteAnalyzer(Analyzer defaultAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            FieldMapper mapper = smartNameFieldMapper(fieldName);
            if (mapper != null && mapper.fieldType().searchQuoteAnalyzer() != null) {
                return mapper.fieldType().searchQuoteAnalyzer();
            }
            return defaultAnalyzer;
        }
    }
}
