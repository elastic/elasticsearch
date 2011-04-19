/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.search.PublicTermsFilter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.xcontent.XContentDocumentMapperParser;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.TypeMissingException;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.collect.MapBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public class MapperService extends AbstractIndexComponent implements Iterable<DocumentMapper> {

    public static final String DEFAULT_MAPPING = "_default_";

    /**
     * Will create types automatically if they do not exists in the mapping definition yet
     */
    private final boolean dynamic;

    private volatile String defaultMappingSource;

    private volatile ImmutableMap<String, DocumentMapper> mappers = ImmutableMap.of();

    private final Object mutex = new Object();

    private volatile ImmutableMap<String, FieldMappers> nameFieldMappers = ImmutableMap.of();
    private volatile ImmutableMap<String, FieldMappers> indexNameFieldMappers = ImmutableMap.of();
    private volatile ImmutableMap<String, FieldMappers> fullNameFieldMappers = ImmutableMap.of();

    // for now, just use the xcontent one. Can work on it more to support custom ones
    private final DocumentMapperParser documentParser;

    private final InternalFieldMapperListener fieldMapperListener = new InternalFieldMapperListener();

    private final SmartIndexNameSearchAnalyzer searchAnalyzer;

    @Inject public MapperService(Index index, @IndexSettings Settings indexSettings, Environment environment, AnalysisService analysisService) {
        super(index, indexSettings);
        this.documentParser = new XContentDocumentMapperParser(index, indexSettings, analysisService);
        this.searchAnalyzer = new SmartIndexNameSearchAnalyzer(analysisService.defaultSearchAnalyzer());

        this.dynamic = componentSettings.getAsBoolean("dynamic", true);
        String defaultMappingLocation = componentSettings.get("default_mapping_location");
        URL defaultMappingUrl;
        if (defaultMappingLocation == null) {
            try {
                defaultMappingUrl = environment.resolveConfig("default-mapping.json");
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                defaultMappingUrl = indexSettings.getClassLoader().getResource("org/elasticsearch/index/mapper/xcontent/default-mapping.json");
            }
        } else {
            try {
                defaultMappingUrl = environment.resolveConfig(defaultMappingLocation);
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                try {
                    defaultMappingUrl = new File(defaultMappingLocation).toURI().toURL();
                } catch (MalformedURLException e1) {
                    throw new FailedToResolveConfigException("Failed to resolve dynamic mapping location [" + defaultMappingLocation + "]");
                }
            }
        }

        try {
            defaultMappingSource = Streams.copyToString(new InputStreamReader(defaultMappingUrl.openStream(), "UTF-8"));
        } catch (IOException e) {
            throw new MapperException("Failed to load default mapping source from [" + defaultMappingLocation + "]", e);
        }

        logger.debug("using dynamic[{}], default mapping: location[{}] and source[{}]", dynamic, defaultMappingLocation, defaultMappingSource);
    }

    public void close() {
        for (DocumentMapper documentMapper : mappers.values()) {
            documentMapper.close();
        }
    }

    @Override public UnmodifiableIterator<DocumentMapper> iterator() {
        return mappers.values().iterator();
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    public void add(String type, String mappingSource) {
        if (DEFAULT_MAPPING.equals(type)) {
            // verify we can parse it
            DocumentMapper mapper = documentParser.parse(type, mappingSource);
            // still add it as a document mapper so we have it registered and, for example, persisted back into
            // the cluster meta data if needed, or checked for existence
            synchronized (mutex) {
                mappers = newMapBuilder(mappers).put(type, mapper).immutableMap();
            }
            defaultMappingSource = mappingSource;
        } else {
            add(parse(type, mappingSource));
        }
    }

    private void add(DocumentMapper mapper) {
        synchronized (mutex) {
            if (mapper.type().charAt(0) == '_') {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] can't start with '_'");
            }
            if (mapper.type().contains("#")) {
                throw new InvalidTypeNameException("mapping type name [" + mapper.type() + "] should not include '#' in it");
            }
            if (mapper.type().contains(".")) {
                logger.warn("Type [{}] contains a '.', it is recommended not to include it within a type name", mapper.type());
            }
            remove(mapper.type()); // first remove it (in case its an update, we need to remove the aggregated mappers)
            mappers = newMapBuilder(mappers).put(mapper.type(), mapper).immutableMap();
            mapper.addFieldMapperListener(fieldMapperListener, true);
        }
    }

    public void remove(String type) {
        synchronized (mutex) {
            DocumentMapper docMapper = mappers.get(type);
            if (docMapper == null) {
                return;
            }
            docMapper.close();
            mappers = newMapBuilder(mappers).remove(type).immutableMap();

            // we need to remove those mappers
            for (FieldMapper mapper : docMapper.mappers()) {
                FieldMappers mappers = nameFieldMappers.get(mapper.names().name());
                if (mappers != null) {
                    mappers = mappers.remove(mapper);
                    if (mappers.isEmpty()) {
                        nameFieldMappers = newMapBuilder(nameFieldMappers).remove(mapper.names().name()).immutableMap();
                    } else {
                        nameFieldMappers = newMapBuilder(nameFieldMappers).put(mapper.names().name(), mappers).immutableMap();
                    }
                }

                mappers = indexNameFieldMappers.get(mapper.names().indexName());
                if (mappers != null) {
                    mappers = mappers.remove(mapper);
                    if (mappers.isEmpty()) {
                        indexNameFieldMappers = newMapBuilder(indexNameFieldMappers).remove(mapper.names().indexName()).immutableMap();
                    } else {
                        indexNameFieldMappers = newMapBuilder(indexNameFieldMappers).put(mapper.names().indexName(), mappers).immutableMap();
                    }
                }

                mappers = fullNameFieldMappers.get(mapper.names().fullName());
                if (mappers != null) {
                    mappers = mappers.remove(mapper);
                    if (mappers.isEmpty()) {
                        fullNameFieldMappers = newMapBuilder(fullNameFieldMappers).remove(mapper.names().fullName()).immutableMap();
                    } else {
                        fullNameFieldMappers = newMapBuilder(fullNameFieldMappers).put(mapper.names().fullName(), mappers).immutableMap();
                    }
                }
            }
        }
    }

    /**
     * Just parses and returns the mapper without adding it.
     */
    public DocumentMapper parse(String mappingType, String mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource, defaultMappingSource);
    }

    public boolean hasMapping(String mappingType) {
        return mappers.containsKey(mappingType);
    }

    public DocumentMapper documentMapper(String type) {
        return mappers.get(type);
    }

    public DocumentMapper documentMapperWithAutoCreate(String type) {
        DocumentMapper mapper = mappers.get(type);
        if (mapper != null) {
            return mapper;
        }
        if (!dynamic) {
            throw new TypeMissingException(index, type, "typing to auto create mapping, but dynamic mapping is disabled");
        }
        // go ahead and dynamically create it
        synchronized (mutex) {
            mapper = mappers.get(type);
            if (mapper != null) {
                return mapper;
            }
            add(type, null);
            return mappers.get(type);
        }
    }

    /**
     * A filter to filter based on several types. Will not throw types missing failure, and will
     * simply filter it out also.
     */
    public Filter typesFilter(String... types) {
        if (types.length == 1) {
            DocumentMapper docMapper = documentMapper(types[0]);
            if (docMapper == null) {
                return new TermFilter(new Term(types[0]));
            }
            return docMapper.typeFilter();
        }
        // see if we can use terms filter
        boolean useTermsFilter = true;
        for (String type : types) {
            DocumentMapper docMapper = documentMapper(type);
            if (docMapper == null) {
                useTermsFilter = false;
                break;
            }
            if (!docMapper.typeMapper().indexed()) {
                useTermsFilter = false;
                break;
            }
        }
        if (useTermsFilter) {
            PublicTermsFilter termsFilter = new PublicTermsFilter();
            for (String type : types) {
                termsFilter.addTerm(new Term(TypeFieldMapper.NAME, type));
            }
            return termsFilter;
        } else {
            XBooleanFilter bool = new XBooleanFilter();
            for (String type : types) {
                DocumentMapper docMapper = documentMapper(type);
                if (docMapper == null) {
                    bool.add(new FilterClause(new TermFilter(new Term(TypeFieldMapper.NAME, type)), BooleanClause.Occur.SHOULD));
                } else {
                    bool.add(new FilterClause(docMapper.typeFilter(), BooleanClause.Occur.SHOULD));
                }
            }
            return bool;
        }
    }

    /**
     * A filter to filter based on several types.
     */
    public Filter typesFilterFailOnMissing(String... types) throws TypeMissingException {
        if (types.length == 1) {
            DocumentMapper docMapper = documentMapper(types[0]);
            if (docMapper == null) {
                throw new TypeMissingException(index, types[0]);
            }
            return docMapper.typeFilter();
        }
        PublicTermsFilter termsFilter = new PublicTermsFilter();
        for (String type : types) {
            if (!hasMapping(type)) {
                throw new TypeMissingException(index, type);
            }
            termsFilter.addTerm(new Term(TypeFieldMapper.NAME, type));
        }
        return termsFilter;
    }

    /**
     * Returns {@link FieldMappers} for all the {@link FieldMapper}s that are registered
     * under the given name across all the different {@link DocumentMapper} types.
     *
     * @param name The name to return all the {@link FieldMappers} for across all {@link DocumentMapper}s.
     * @return All the {@link FieldMappers} for across all {@link DocumentMapper}s
     */
    public FieldMappers name(String name) {
        return nameFieldMappers.get(name);
    }

    /**
     * Returns {@link FieldMappers} for all the {@link FieldMapper}s that are registered
     * under the given indexName across all the different {@link DocumentMapper} types.
     *
     * @param indexName The indexName to return all the {@link FieldMappers} for across all {@link DocumentMapper}s.
     * @return All the {@link FieldMappers} across all {@link DocumentMapper}s for the given indexName.
     */
    public FieldMappers indexName(String indexName) {
        return indexNameFieldMappers.get(indexName);
    }

    /**
     * Returns the {@link FieldMappers} of all the {@link FieldMapper}s that are
     * registered under the give fullName across all the different {@link DocumentMapper} types.
     *
     * @param fullName The full name
     * @return All teh {@link FieldMappers} across all the {@link DocumentMapper}s for the given fullName.
     */
    public FieldMappers fullName(String fullName) {
        return fullNameFieldMappers.get(fullName);
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

    public Set<String> simpleMatchToIndexNames(String pattern) {
        int dotIndex = pattern.indexOf('.');
        if (dotIndex != -1) {
            String possibleType = pattern.substring(0, dotIndex);
            DocumentMapper possibleDocMapper = mappers.get(possibleType);
            if (possibleDocMapper != null) {
                Set<String> typedFields = Sets.newHashSet();
                for (String indexName : possibleDocMapper.mappers().simpleMatchToIndexNames(pattern)) {
                    typedFields.add(possibleType + "." + indexName);
                }
                return typedFields;
            }
        }
        Set<String> fields = Sets.newHashSet();
        for (Map.Entry<String, FieldMappers> entry : fullNameFieldMappers.entrySet()) {
            if (Regex.simpleMatch(pattern, entry.getKey())) {
                for (FieldMapper mapper : entry.getValue()) {
                    fields.add(mapper.names().indexName());
                }
            }
        }
        for (Map.Entry<String, FieldMappers> entry : indexNameFieldMappers.entrySet()) {
            if (Regex.simpleMatch(pattern, entry.getKey())) {
                for (FieldMapper mapper : entry.getValue()) {
                    fields.add(mapper.names().indexName());
                }
            }
        }
        for (Map.Entry<String, FieldMappers> entry : nameFieldMappers.entrySet()) {
            if (Regex.simpleMatch(pattern, entry.getKey())) {
                for (FieldMapper mapper : entry.getValue()) {
                    fields.add(mapper.names().indexName());
                }
            }
        }
        return fields;
    }

    /**
     * Same as {@link #smartName(String)}, except it returns just the field mappers.
     */
    public FieldMappers smartNameFieldMappers(String smartName) {
        int dotIndex = smartName.indexOf('.');
        if (dotIndex != -1) {
            String possibleType = smartName.substring(0, dotIndex);
            DocumentMapper possibleDocMapper = mappers.get(possibleType);
            if (possibleDocMapper != null) {
                String possibleName = smartName.substring(dotIndex + 1);
                FieldMappers mappers = possibleDocMapper.mappers().smartName(possibleName);
                if (mappers != null) {
                    return mappers;
                }
            }
        }
        FieldMappers mappers = fullName(smartName);
        if (mappers != null) {
            return mappers;
        }
        mappers = indexName(smartName);
        if (mappers != null) {
            return mappers;
        }
        return name(smartName);
    }

    /**
     * Returns smart field mappers based on a smart name. A smart name is one that can optioannly be prefixed
     * with a type (and then a '.'). If it is, then the {@link MapperService.SmartNameFieldMappers}
     * will have the doc mapper set.
     *
     * <p>It also (without the optional type prefix) try and find the {@link FieldMappers} for the specific
     * name. It will first try to find it based on the full name (with the dots if its a compound name). If
     * it is not found, will try and find it based on the indexName (which can be controlled in the mapping),
     * and last, will try it based no the name itself.
     *
     * <p>If nothing is found, returns null.
     */
    public SmartNameFieldMappers smartName(String smartName) {
        int dotIndex = smartName.indexOf('.');
        if (dotIndex != -1) {
            String possibleType = smartName.substring(0, dotIndex);
            DocumentMapper possibleDocMapper = mappers.get(possibleType);
            if (possibleDocMapper != null) {
                String possibleName = smartName.substring(dotIndex + 1);
                FieldMappers mappers = possibleDocMapper.mappers().smartName(possibleName);
                if (mappers != null) {
                    return new SmartNameFieldMappers(mappers, possibleDocMapper);
                }
            }
        }
        FieldMappers fieldMappers = fullName(smartName);
        if (fieldMappers != null) {
            return new SmartNameFieldMappers(fieldMappers, null);
        }
        fieldMappers = indexName(smartName);
        if (fieldMappers != null) {
            return new SmartNameFieldMappers(fieldMappers, null);
        }
        fieldMappers = name(smartName);
        if (fieldMappers != null) {
            return new SmartNameFieldMappers(fieldMappers, null);
        }
        return null;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public static class SmartNameFieldMappers {
        private final FieldMappers fieldMappers;
        private final DocumentMapper docMapper;

        public SmartNameFieldMappers(FieldMappers fieldMappers, @Nullable DocumentMapper docMapper) {
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
    }

    final class SmartIndexNameSearchAnalyzer extends Analyzer {

        private final Analyzer defaultAnalyzer;

        SmartIndexNameSearchAnalyzer(Analyzer defaultAnalyzer) {
            this.defaultAnalyzer = defaultAnalyzer;
        }

        @Override public int getPositionIncrementGap(String fieldName) {
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().getPositionIncrementGap(fieldName);
                }
            }
            FieldMappers mappers = fullNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().getPositionIncrementGap(fieldName);
            }

            mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().getPositionIncrementGap(fieldName);
            }
            return defaultAnalyzer.getPositionIncrementGap(fieldName);
        }

        @Override public int getOffsetGap(Fieldable field) {
            String fieldName = field.name();
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().getOffsetGap(field);
                }
            }
            FieldMappers mappers = fullNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().getOffsetGap(field);
            }

            mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().getOffsetGap(field);
            }
            return defaultAnalyzer.getOffsetGap(field);
        }

        @Override public final TokenStream tokenStream(String fieldName, Reader reader) {
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().tokenStream(fieldName, reader);
                }
            }
            FieldMappers mappers = fullNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().tokenStream(fieldName, reader);
            }

            mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().tokenStream(fieldName, reader);
            }
            return defaultAnalyzer.tokenStream(fieldName, reader);
        }

        @Override public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().reusableTokenStream(fieldName, reader);
                }
            }
            FieldMappers mappers = fullNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().reusableTokenStream(fieldName, reader);
            }

            mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().reusableTokenStream(fieldName, reader);
            }
            return defaultAnalyzer.reusableTokenStream(fieldName, reader);
        }
    }

    private class InternalFieldMapperListener implements FieldMapperListener {
        @Override public void fieldMapper(FieldMapper fieldMapper) {
            synchronized (mutex) {
                FieldMappers mappers = nameFieldMappers.get(fieldMapper.names().name());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }

                nameFieldMappers = newMapBuilder(nameFieldMappers).put(fieldMapper.names().name(), mappers).immutableMap();

                mappers = indexNameFieldMappers.get(fieldMapper.names().indexName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                indexNameFieldMappers = newMapBuilder(indexNameFieldMappers).put(fieldMapper.names().indexName(), mappers).immutableMap();

                mappers = fullNameFieldMappers.get(fieldMapper.names().fullName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                fullNameFieldMappers = newMapBuilder(fullNameFieldMappers).put(fieldMapper.names().fullName(), mappers).immutableMap();
            }
        }
    }
}
