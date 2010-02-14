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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexLifecycle;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.json.JsonDocumentMapperParser;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.io.Streams;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;

import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
@IndexLifecycle
@ThreadSafe
public class MapperService extends AbstractIndexComponent implements Iterable<DocumentMapper> {

    /**
     * Will create types automatically if they do not exists in the repo yet
     */
    private final boolean dynamic;

    private final String dynamicMappingLocation;

    private final URL dynamicMappingUrl;

    private final ClassLoader indexClassLoader;

    private final String dynamicMappingSource;

    private volatile ImmutableMap<String, DocumentMapper> mappers = ImmutableMap.of();

    private final Object mutex = new Object();

    private volatile ImmutableMap<String, FieldMappers> nameFieldMappers = ImmutableMap.of();
    private volatile ImmutableMap<String, FieldMappers> indexNameFieldMappers = ImmutableMap.of();
    private volatile ImmutableMap<String, FieldMappers> fullNameFieldMappers = ImmutableMap.of();
    private volatile FieldMappers idFieldMappers = new FieldMappers();
    private volatile FieldMappers typeFieldMappers = new FieldMappers();
    private volatile FieldMappers uidFieldMappers = new FieldMappers();
    private volatile FieldMappers sourceFieldMappers = new FieldMappers();

    // for now, just use the json one. Can work on it more to support custom ones
    private final DocumentMapperParser documentParser;

    private final InternalFieldMapperListener fieldMapperListener = new InternalFieldMapperListener();

    private final SmartIndexNameSearchAnalyzer searchAnalyzer;

    @Inject public MapperService(Index index, @IndexSettings Settings indexSettings, Environment environment, AnalysisService analysisService) {
        super(index, indexSettings);
        this.documentParser = new JsonDocumentMapperParser(analysisService);
        this.searchAnalyzer = new SmartIndexNameSearchAnalyzer(analysisService.defaultSearchAnalyzer());
        this.indexClassLoader = indexSettings.getClassLoader();

        this.dynamic = componentSettings.getAsBoolean("dynamic", true);
        String dynamicMappingLocation = componentSettings.get("dynamicMappingLocation");
        URL dynamicMappingUrl;
        if (dynamicMappingLocation == null) {
            try {
                dynamicMappingUrl = environment.resolveConfig("dynamic-mapping.json");
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                dynamicMappingUrl = indexClassLoader.getResource("org/elasticsearch/index/mapper/json/dynamic-mapping.json");
            }
        } else {
            try {
                dynamicMappingUrl = environment.resolveConfig(dynamicMappingLocation);
            } catch (FailedToResolveConfigException e) {
                // not there, default to the built in one
                try {
                    dynamicMappingUrl = new File(dynamicMappingLocation).toURI().toURL();
                } catch (MalformedURLException e1) {
                    throw new FailedToResolveConfigException("Failed to resolve dynamic mapping location [" + dynamicMappingLocation + "]");
                }
            }
        }
        this.dynamicMappingUrl = dynamicMappingUrl;
        if (dynamicMappingLocation == null) {
            this.dynamicMappingLocation = dynamicMappingUrl.toExternalForm();
        } else {
            this.dynamicMappingLocation = dynamicMappingLocation;
        }

        if (dynamic) {
            try {
                dynamicMappingSource = Streams.copyToString(new InputStreamReader(dynamicMappingUrl.openStream(), "UTF8"));
            } catch (IOException e) {
                throw new MapperException("Failed to load default mapping source from [" + dynamicMappingLocation + "]", e);
            }
        } else {
            dynamicMappingSource = null;
        }
        logger.debug("Using dynamic [{}] with location [{}] and source [{}]", new Object[]{dynamic, dynamicMappingLocation, dynamicMappingSource});
    }

    @Override public UnmodifiableIterator<DocumentMapper> iterator() {
        return mappers.values().iterator();
    }

    public DocumentMapper type(String type) {
        DocumentMapper mapper = mappers.get(type);
        if (mapper != null) {
            return mapper;
        }
        if (!dynamic) {
            return null;
        }
        // go ahead and dynamically create it
        synchronized (mutex) {
            mapper = mappers.get(type);
            if (mapper != null) {
                return mapper;
            }
            add(type, dynamicMappingSource);
            return mappers.get(type);
        }
    }

    public void add(String type, String mappingSource) {
        add(documentParser.parse(type, mappingSource));
    }

    public void add(String mappingSource) throws MapperParsingException {
        add(documentParser.parse(mappingSource));
    }

    /**
     * Just parses and returns the mapper without adding it.
     */
    public DocumentMapper parse(String mappingType, String mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource);
    }

    public boolean hasMapping(String mappingType) {
        return mappers.containsKey(mappingType);
    }

    public DocumentMapper documentMapper(String type) {
        return mappers.get(type);
    }

    public FieldMappers idFieldMappers() {
        return this.idFieldMappers;
    }

    public FieldMappers typeFieldMappers() {
        return this.typeFieldMappers;
    }

    public FieldMappers sourceFieldMappers() {
        return this.sourceFieldMappers;
    }

    public FieldMappers uidFieldMappers() {
        return this.uidFieldMappers;
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
     * registered under the give fullName ({@link FieldMapper#fullName()} across
     * all the different {@link DocumentMapper} types.
     *
     * @param fullName The full name
     * @return All teh {@link FieldMappers} across all the {@link DocumentMapper}s for the given fullName.
     */
    public FieldMappers fullName(String fullName) {
        return fullNameFieldMappers.get(fullName);
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
                FieldMappers mappers = possibleDocMapper.mappers().fullName(possibleName);
                if (mappers != null) {
                    return mappers;
                }
                mappers = possibleDocMapper.mappers().indexName(possibleName);
                if (mappers != null) {
                    return mappers;
                }
            }
        }
        FieldMappers mappers = fullName(smartName);
        if (mappers != null) {
            return mappers;
        }
        return indexName(smartName);
    }

    /**
     * Returns smart field mappers based on a smart name. A smart name is one that can optioannly be prefixed
     * with a type (and then a '.'). If it is, then the {@link MapperService.SmartNameFieldMappers}
     * will have the doc mapper set.
     *
     * <p>It also (without the optional type prefix) try and find the {@link FieldMappers} for the specific
     * name. It will first try to find it based on the full name (with the dots if its a compound name). If
     * it is not found, will try and find it based on the indexName (which can be controlled in the mapping).
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
                FieldMappers mappers = possibleDocMapper.mappers().fullName(possibleName);
                if (mappers != null) {
                    return new SmartNameFieldMappers(mappers, possibleDocMapper);
                }
                mappers = possibleDocMapper.mappers().indexName(possibleName);
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
        return null;
    }

    public void add(DocumentMapper mapper) {
        synchronized (mutex) {
            if (mapper.type().charAt(0) == '_') {
                throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
            }
            mappers = newMapBuilder(mappers).put(mapper.type(), mapper).immutableMap();
            mapper.addFieldMapperListener(fieldMapperListener, true);
        }
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

        public FieldMappers fieldMappers() {
            return fieldMappers;
        }

        public boolean hasDocMapper() {
            return docMapper != null;
        }

        public DocumentMapper docMapper() {
            return docMapper;
        }
    }

    private class SmartIndexNameSearchAnalyzer extends Analyzer {

        private final Analyzer defaultAnalyzer;

        private SmartIndexNameSearchAnalyzer(Analyzer defaultAnalyzer) {
            this.defaultAnalyzer = defaultAnalyzer;
        }

        @Override public TokenStream tokenStream(String fieldName, Reader reader) {
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

        @Override public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
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
                if (fieldMapper instanceof IdFieldMapper) {
                    idFieldMappers = idFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof TypeFieldMapper) {
                    typeFieldMappers = typeFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof SourceFieldMapper) {
                    sourceFieldMappers = sourceFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof UidFieldMapper) {
                    uidFieldMappers = uidFieldMappers.concat(fieldMapper);
                }


                FieldMappers mappers = nameFieldMappers.get(fieldMapper.name());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }

                nameFieldMappers = newMapBuilder(nameFieldMappers).put(fieldMapper.name(), mappers).immutableMap();

                mappers = indexNameFieldMappers.get(fieldMapper.indexName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                indexNameFieldMappers = newMapBuilder(indexNameFieldMappers).put(fieldMapper.indexName(), mappers).immutableMap();

                mappers = fullNameFieldMappers.get(fieldMapper.indexName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                fullNameFieldMappers = newMapBuilder(fullNameFieldMappers).put(fieldMapper.fullName(), mappers).immutableMap();
            }
        }
    }
}
