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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class MapperService extends AbstractIndexComponent  {

    public static final String DEFAULT_MAPPING = "_default_";
    private static ObjectOpenHashSet<String> META_FIELDS = ObjectOpenHashSet.from(
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

    private final Object typeMutex = new Object();
    private final Object mappersMutex = new Object();

    private volatile FieldMappersLookup fieldMappers;
    private volatile ImmutableOpenMap<String, ObjectMappers> fullPathObjectMappers = ImmutableOpenMap.of();
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

    private final InternalFieldMapperListener fieldMapperListener = new InternalFieldMapperListener();
    private final InternalObjectMapperListener objectMapperListener = new InternalObjectMapperListener();

    private final SmartIndexNameSearchAnalyzer searchAnalyzer;
    private final SmartIndexNameSearchQuoteAnalyzer searchQuoteAnalyzer;

    private final List<DocumentTypeListener> typeListeners = new CopyOnWriteArrayList<>();

    private volatile ImmutableMap<String, FieldMapper<?>> unmappedFieldMappers = ImmutableMap.of();

    @Inject
    public MapperService(Index index, @IndexSettings Settings indexSettings, Environment environment, AnalysisService analysisService, IndexFieldDataService fieldDataService,
                         SimilarityLookupService similarityLookupService,
                         ScriptService scriptService) {
        super(index, indexSettings);
        this.analysisService = analysisService;
        this.fieldDataService = fieldDataService;
        this.fieldMappers = new FieldMappersLookup();
        this.documentParser = new DocumentMapperParser(index, indexSettings, analysisService, similarityLookupService, scriptService);
        this.searchAnalyzer = new SmartIndexNameSearchAnalyzer(analysisService.defaultSearchAnalyzer());
        this.searchQuoteAnalyzer = new SmartIndexNameSearchQuoteAnalyzer(analysisService.defaultSearchQuoteAnalyzer());

        this.dynamic = indexSettings.getAsBoolean("index.mapper.dynamic", true);
        String defaultMappingLocation = indexSettings.get("index.mapper.default_mapping_location");
        final URL defaultMappingUrl;
        if (index.getName().equals(ScriptService.SCRIPT_INDEX)){
            defaultMappingUrl = getMappingUrl(indexSettings, environment, defaultMappingLocation, "script-mapping.json", "org/elasticsearch/index/mapper/script-mapping.json");
        } else {
            defaultMappingUrl = getMappingUrl(indexSettings, environment, defaultMappingLocation, "default-mapping.json", "org/elasticsearch/index/mapper/default-mapping.json");
        }

        if (defaultMappingUrl == null) {
            logger.info("failed to find default-mapping.json in the classpath, using the default template");
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
                defaultMappingSource = "{\n" +
                        "    \"_default_\":{\n" +
                        "    }\n" +
                        "}";
            }
        } else {
            try {
                defaultMappingSource = Streams.copyToString(FileSystemUtils.newBufferedReader(defaultMappingUrl, Charsets.UTF_8));
            } catch (IOException e) {
                throw new MapperException("Failed to load default mapping source from [" + defaultMappingLocation + "]", e);
            }
        }

        String percolatorMappingLocation = indexSettings.get("index.mapper.default_percolator_mapping_location");
        URL percolatorMappingUrl = null;
        if (percolatorMappingLocation != null) {
            try {
                percolatorMappingUrl = environment.resolveConfig(percolatorMappingLocation);
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                try {
                    percolatorMappingUrl = PathUtils.get(percolatorMappingLocation).toUri().toURL();
                } catch (MalformedURLException e1) {
                    throw new FailedToResolveConfigException("Failed to resolve default percolator mapping location [" + percolatorMappingLocation + "]");
                }
            }
        }
        if (percolatorMappingUrl != null) {
            try {
                defaultPercolatorMappingSource = Streams.copyToString(FileSystemUtils.newBufferedReader(percolatorMappingUrl, Charsets.UTF_8));
            } catch (IOException e) {
                throw new MapperException("Failed to load default percolator mapping source from [" + percolatorMappingUrl + "]", e);
            }
        } else {
            defaultPercolatorMappingSource = "{\n" +
                    //"    \"" + PercolatorService.TYPE_NAME + "\":{\n" +
                    "    \"" + "_default_" + "\":{\n" +
                    "        \"properties\" : {\n" +
                    "            \"query\" : {\n" +
                    "                \"type\" : \"object\",\n" +
                    "                \"enabled\" : false\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";
        }

        if (logger.isTraceEnabled()) {
            logger.trace("using dynamic[{}], default mapping: default_mapping_location[{}], loaded_from[{}] and source[{}], default percolator mapping: location[{}], loaded_from[{}] and source[{}]", dynamic, defaultMappingLocation, defaultMappingUrl, defaultMappingSource, percolatorMappingLocation, percolatorMappingUrl, defaultPercolatorMappingSource);
        } else if (logger.isDebugEnabled()) {
            logger.debug("using dynamic[{}], default mapping: default_mapping_location[{}], loaded_from[{}], default percolator mapping: location[{}], loaded_from[{}]", dynamic, defaultMappingLocation, defaultMappingUrl, percolatorMappingLocation, percolatorMappingUrl);
        }
    }

    private URL getMappingUrl(Settings indexSettings, Environment environment, String mappingLocation, String configString, String resourceLocation) {
        URL mappingUrl;
        if (mappingLocation == null) {
            try {
                mappingUrl = environment.resolveConfig(configString);
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                mappingUrl = indexSettings.getClassLoader().getResource(resourceLocation);
                if (mappingUrl == null) {
                    mappingUrl = MapperService.class.getClassLoader().getResource(resourceLocation);
                }
            }
        } else {
            try {
                mappingUrl = environment.resolveConfig(mappingLocation);
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                try {
                    mappingUrl = PathUtils.get(mappingLocation).toUri().toURL();
                } catch (MalformedURLException e1) {
                    throw new FailedToResolveConfigException("Failed to resolve dynamic mapping location [" + mappingLocation + "]");
                }
            }
        }
        return mappingUrl;
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

    public DocumentMapper merge(String type, CompressedString mappingSource, boolean applyDefault) {
        if (DEFAULT_MAPPING.equals(type)) {
            // verify we can parse it
            DocumentMapper mapper = documentParser.parseCompressed(type, mappingSource);
            // still add it as a document mapper so we have it registered and, for example, persisted back into
            // the cluster meta data if needed, or checked for existence
            synchronized (typeMutex) {
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
        synchronized (typeMutex) {
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
                return oldMapper;
            } else {
                FieldMapperListener.Aggregator fieldMappersAgg = new FieldMapperListener.Aggregator();
                mapper.traverse(fieldMappersAgg);
                addFieldMappers(fieldMappersAgg.mappers);
                mapper.addFieldMapperListener(fieldMapperListener);

                ObjectMapperListener.Aggregator objectMappersAgg = new ObjectMapperListener.Aggregator();
                mapper.traverse(objectMappersAgg);
                addObjectMappers(objectMappersAgg.mappers.toArray(new ObjectMapper[objectMappersAgg.mappers.size()]));
                mapper.addObjectMapperListener(objectMapperListener);

                for (DocumentTypeListener typeListener : typeListeners) {
                    typeListener.beforeCreate(mapper);
                }
                mappers = newMapBuilder(mappers).put(mapper.type(), mapper).map();
                return mapper;
            }
        }
    }

    private void addObjectMappers(ObjectMapper[] objectMappers) {
        synchronized (mappersMutex) {
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
    }

    private void addFieldMappers(Collection<FieldMapper<?>> fieldMappers) {
        synchronized (mappersMutex) {
            this.fieldMappers = this.fieldMappers.copyAndAddAll(fieldMappers);
        }
    }

    public DocumentMapper parse(String mappingType, CompressedString mappingSource, boolean applyDefault) throws MapperParsingException {
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
    public Filter searchFilter(String... types) {
        boolean filterPercolateType = hasMapping(PercolatorService.TYPE_NAME);
        if (types != null && filterPercolateType) {
            for (String type : types) {
                if (PercolatorService.TYPE_NAME.equals(type)) {
                    filterPercolateType = false;
                    break;
                }
            }
        }
        Filter percolatorType = null;
        if (filterPercolateType) {
            percolatorType = documentMapper(PercolatorService.TYPE_NAME).typeFilter();
        }

        if (types == null || types.length == 0) {
            if (hasNested && filterPercolateType) {
                BooleanQuery bq = new BooleanQuery();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(Queries.newNonNestedFilter(), Occur.MUST);
                return Queries.wrap(bq);
            } else if (hasNested) {
                return Queries.newNonNestedFilter();
            } else if (filterPercolateType) {
                return Queries.wrap(Queries.not(percolatorType));
            } else {
                return null;
            }
        }
        // if we filter by types, we don't need to filter by non nested docs
        // since they have different types (starting with __)
        if (types.length == 1) {
            DocumentMapper docMapper = documentMapper(types[0]);
            Filter filter = docMapper != null ? docMapper.typeFilter() : Queries.wrap(new TermQuery(new Term(TypeFieldMapper.NAME, types[0])));
            if (filterPercolateType) {
                BooleanQuery bq = new BooleanQuery();
                bq.add(percolatorType, Occur.MUST_NOT);
                bq.add(filter, Occur.MUST);
                return Queries.wrap(bq);
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
                return Queries.wrap(bq);
            } else {
                return Queries.wrap(termsFilter);
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

            return Queries.wrap(bool);
        }
    }

    /**
     * Returns {@link FieldMappers} for all the {@link FieldMapper}s that are registered
     * under the given indexName across all the different {@link DocumentMapper} types.
     *
     * @param indexName The indexName to return all the {@link FieldMappers} for across all {@link DocumentMapper}s.
     * @return All the {@link FieldMappers} across all {@link DocumentMapper}s for the given indexName.
     */
    public FieldMappers indexName(String indexName) {
        return fieldMappers.indexName(indexName);
    }

    /**
     * Returns the {@link FieldMappers} of all the {@link FieldMapper}s that are
     * registered under the give fullName across all the different {@link DocumentMapper} types.
     *
     * @param fullName The full name
     * @return All teh {@link FieldMappers} across all the {@link DocumentMapper}s for the given fullName.
     */
    public FieldMappers fullName(String fullName) {
        return fieldMappers.fullName(fullName);
    }

    /**
     * Returns objects mappers based on the full path of the object.
     */
    public ObjectMappers objectMapper(String path) {
        return fullPathObjectMappers.get(path);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     */
    public List<String> simpleMatchToIndexNames(String pattern) {
        return simpleMatchToIndexNames(pattern, null);
    }
    /**
     * Returns all the fields that match the given pattern, with an optional narrowing
     * based on a list of types.
     */
    public List<String> simpleMatchToIndexNames(String pattern, @Nullable String[] types) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return ImmutableList.of(pattern);
        }
        
        if (types == null || types.length == 0 || types.length == 1 && types[0].equals("_all")) {
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
            ObjectMappers mappers = objectMapper(smartName);
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

    /**
     * Same as {@link #smartNameFieldMappers(String)} but returns the first field mapper for it. Returns
     * <tt>null</tt> if there is none.
     */
    public FieldMapper smartNameFieldMapper(String smartName) {
        FieldMappers fieldMappers = smartNameFieldMappers(smartName);
        if (fieldMappers != null) {
            return fieldMappers.mapper();
        }
        return null;
    }

    public FieldMapper smartNameFieldMapper(String smartName, @Nullable String[] types) {
        FieldMappers fieldMappers = smartNameFieldMappers(smartName, types);
        if (fieldMappers != null) {
            return fieldMappers.mapper();
        }
        return null;
    }

    public FieldMappers smartNameFieldMappers(String smartName, @Nullable String[] types) {
        if (types == null || types.length == 0) {
            return smartNameFieldMappers(smartName);
        }
        for (String type : types) {
            DocumentMapper documentMapper = mappers.get(type);
            // we found a mapper
            if (documentMapper != null) {
                // see if we find a field for it
                FieldMappers mappers = documentMapper.mappers().smartName(smartName);
                if (mappers != null) {
                    return mappers;
                }
            }
        }
        return null;
    }

    /**
     * Same as {@link #smartName(String)}, except it returns just the field mappers.
     */
    public FieldMappers smartNameFieldMappers(String smartName) {
        FieldMappers mappers = fullName(smartName);
        if (mappers != null) {
            return mappers;
        }
        return indexName(smartName);
    }

    public SmartNameFieldMappers smartName(String smartName, @Nullable String[] types) {
        if (types == null || types.length == 0) {
            return smartName(smartName);
        }
        if (types.length == 1 && types[0].equals("_all")) {
            return smartName(smartName);
        }
        for (String type : types) {
            DocumentMapper documentMapper = mappers.get(type);
            // we found a mapper
            if (documentMapper != null) {
                // see if we find a field for it
                FieldMappers mappers = documentMapper.mappers().smartName(smartName);
                if (mappers != null) {
                    return new SmartNameFieldMappers(this, mappers, documentMapper, false);
                }
            }
        }
        return null;
    }

    /**
     * Returns smart field mappers based on a smart name. A smart name is any of full name or index name.
     * <p/>
     * <p>It will first try to find it based on the full name (with the dots if its a compound name). If
     * it is not found, will try and find it based on the indexName (which can be controlled in the mapping).
     * <p/>
     * <p>If nothing is found, returns null.
     */
    public SmartNameFieldMappers smartName(String smartName) {
        FieldMappers fieldMappers = fullName(smartName);
        if (fieldMappers != null) {
            return new SmartNameFieldMappers(this, fieldMappers, null, false);
        }
        fieldMappers = indexName(smartName);
        if (fieldMappers != null) {
            return new SmartNameFieldMappers(this, fieldMappers, null, false);
        }
        return null;
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public FieldMapper<?> unmappedFieldMapper(String type) {
        final ImmutableMap<String, FieldMapper<?>> unmappedFieldMappers = this.unmappedFieldMappers;
        FieldMapper<?> mapper = unmappedFieldMappers.get(type);
        if (mapper == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new ElasticsearchIllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?, ?> builder = typeParser.parse("__anonymous_" + type, ImmutableMap.<String, Object>of(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
            mapper = (FieldMapper<?>) builder.build(builderContext);

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            this.unmappedFieldMappers = ImmutableMap.<String, FieldMapper<?>>builder()
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
                ObjectMappers objectMappers = objectMapper(objectPath);
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

    public static class SmartNameFieldMappers {
        private final MapperService mapperService;
        private final FieldMappers fieldMappers;
        private final DocumentMapper docMapper;

        public SmartNameFieldMappers(MapperService mapperService, FieldMappers fieldMappers, @Nullable DocumentMapper docMapper, boolean explicitTypeInName) {
            this.mapperService = mapperService;
            this.fieldMappers = fieldMappers;
            this.docMapper = docMapper;
        }

        /**
         * Has at least one mapper for the field.
         */
        public boolean hasMapper() {
            return !fieldMappers.isEmpty();
        }

        /**
         * The first mapper for the smart named field.
         */
        public FieldMapper mapper() {
            return fieldMappers.mapper();
        }

        /**
         * All the field mappers for the smart name field.
         */
        public FieldMappers fieldMappers() {
            return fieldMappers;
        }

        /**
         * If the smart name was a typed field, with a type that we resolved, will return
         * <tt>true</tt>.
         */
        public boolean hasDocMapper() {
            return docMapper != null;
        }

        /**
         * If the smart name was a typed field, with a type that we resolved, will return
         * the document mapper for it.
         */
        public DocumentMapper docMapper() {
            return docMapper;
        }

        /**
         * The best effort search analyzer associated with this field.
         */
        public Analyzer searchAnalyzer() {
            if (hasMapper()) {
                Analyzer analyzer = mapper().searchAnalyzer();
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return mapperService.searchAnalyzer();
        }

        public Analyzer searchQuoteAnalyzer() {
            if (hasMapper()) {
                Analyzer analyzer = mapper().searchQuoteAnalyzer();
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return mapperService.searchQuoteAnalyzer();
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
            FieldMappers mappers = fieldMappers.fullName(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer();
            }

            mappers = fieldMappers.indexName(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer();
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
            FieldMappers mappers = fieldMappers.fullName(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchQuoteAnalyzer() != null) {
                return mappers.mapper().searchQuoteAnalyzer();
            }

            mappers = fieldMappers.indexName(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchQuoteAnalyzer() != null) {
                return mappers.mapper().searchQuoteAnalyzer();
            }
            return defaultAnalyzer;
        }
    }

    class InternalFieldMapperListener extends FieldMapperListener {
        @Override
        public void fieldMapper(FieldMapper<?> fieldMapper) {
            addFieldMappers(Collections.<FieldMapper<?>>singletonList(fieldMapper));
        }

        @Override
        public void fieldMappers(Collection<FieldMapper<?>> fieldMappers) {
            addFieldMappers(fieldMappers);
        }
    }

    class InternalObjectMapperListener extends ObjectMapperListener {
        @Override
        public void objectMapper(ObjectMapper objectMapper) {
            addObjectMappers(new ObjectMapper[]{objectMapper});
        }

        @Override
        public void objectMappers(ObjectMapper... objectMappers) {
            addObjectMappers(objectMappers);
        }
    }
}
