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

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

public class DocumentMapperParser {

    private static final String PROPERTIES_KEY = "properties";

    final MapperService mapperService;
    final AnalysisService analysisService;
    private final SimilarityService similarityService;
    private final Supplier<QueryShardContext> queryShardContextSupplier;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Version indexVersionCreated;
    private final ParseFieldMatcher parseFieldMatcher;

    private final Map<String, Mapper.TypeParser> typeParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;

    public DocumentMapperParser(IndexSettings indexSettings, MapperService mapperService, AnalysisService analysisService,
                                SimilarityService similarityService, MapperRegistry mapperRegistry,
                                Supplier<QueryShardContext> queryShardContextSupplier) {
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings.getSettings());
        this.mapperService = mapperService;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.queryShardContextSupplier = queryShardContextSupplier;
        this.typeParsers = mapperRegistry.getMapperParsers();
        this.rootTypeParsers = mapperRegistry.getMetadataMapperParsers();
        indexVersionCreated = indexSettings.getIndexVersionCreated();
    }

    public Mapper.TypeParser.ParserContext parserContext(String type) {
        return new Mapper.TypeParser.ParserContext(type, analysisService, similarityService::getSimilarity, mapperService, typeParsers::get, indexVersionCreated, parseFieldMatcher, queryShardContextSupplier.get());
    }

    public DocumentMapper parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        return parse(type, source, null);
    }

    public DocumentMapper parse(@Nullable String type, CompressedXContent source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Map<String, Object> root = XContentHelper.convertToMap(source.compressedReference(), true).v2();
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = new HashMap<>();
        }
        return parse(type, mapping, defaultSource);
    }

    @SuppressWarnings({"unchecked"})
    DocumentMapper parse(String type, Map<String, Object> mapping, String defaultSource) throws MapperParsingException {
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        if (defaultSource != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(MapperService.DEFAULT_MAPPING, defaultSource);
            if (t.v2() != null) {
                XContentHelper.mergeDefaults(mapping, t.v2());
            }
        }


        Mapper.TypeParser.ParserContext parserContext = parserContext(type);
        // parse RootObjectMapper
        DocumentMapper.Builder docBuilder = new DocumentMapper.Builder((RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext), mapperService);
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            MetadataFieldMapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                docBuilder.put(typeParser.parse(fieldName, fieldNodeMap, parserContext));
                fieldNodeMap.remove("type");
                checkNoRemainingFields(fieldName, fieldNodeMap, parserContext.indexVersionCreated());
            }
        }

        Map<String, Object> meta = (Map<String, Object>) mapping.remove("_meta");
        if (meta != null) {
            // It may not be required to copy meta here to maintain immutability
            // but the cost is pretty low here.
            docBuilder.meta(unmodifiableMap(new HashMap<>(meta)));
        }

        checkNoRemainingFields(mapping, parserContext.indexVersionCreated(), "Root mapping definition has unsupported parameters: ");

        return docBuilder.build(mapperService);
    }

    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(fieldNodeMap, indexVersionCreated, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<?, ?> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            throw new MapperParsingException(message + getRemainingFields(fieldNodeMap));
        }
    }

    private static String getRemainingFields(Map<?, ?> map) {
        StringBuilder remainingFields = new StringBuilder();
        for (Object key : map.keySet()) {
            remainingFields.append(" [").append(key).append(" : ").append(map.get(key)).append("]");
        }
        return remainingFields.toString();
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            root = parser.mapOrdered();
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse mapping definition", e);
        }
        return extractMapping(type, root);
    }

    @SuppressWarnings({"unchecked"})
    private Tuple<String, Map<String, Object>> extractMapping(String type, Map<String, Object> root) throws MapperParsingException {
        if (root.size() == 0) {
            // if we don't have any keys throw an exception
            throw new MapperParsingException("malformed mapping no root object found");
        }
        String actualType;
        Map<String, Object> mapping;

        String rootName = root.keySet().iterator().next();
        if (type == null || type.equals(rootName)) {
            actualType = rootName;
            mapping = (Map<String, Object>) root.get(rootName);
        } else {
            actualType = type;
            mapping = root;
        }

        mapping = removeDotsInFieldNames(mapping);
        return new Tuple<>(actualType, mapping);
    }

    static Map<String,Object> removeDotsInFieldNames(Map<String, ?> mapping) {
        Map<String, Object> result = new HashMap<>(mapping);
        Object propertiesObject = result.get(PROPERTIES_KEY);
        if (propertiesObject instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> properties = (Map<String, ?>) propertiesObject;
            properties = removeDotsInFieldNamesFromProperties(properties);
            result.put(PROPERTIES_KEY, properties);
        }
        return result;
    }

    /**
     * Called when we are right under a `properties` object.
     */
    private static Map<String,Object> removeDotsInFieldNamesFromProperties(Map<String, ?> mapping) {
        List<Map.Entry<String, ?>> fieldsWithDotsInFieldNames = new ArrayList<>();
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, ?> entry : mapping.entrySet()) {
            final String fieldName = entry.getKey();
            if (fieldName.contains(".")) {
                fieldsWithDotsInFieldNames.add(entry);
            } else {
                Object fieldMapping = entry.getValue();
                if (fieldMapping instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMappingAsMap = (Map<String, Object>) fieldMapping;
                    if (fieldMappingAsMap.containsKey(PROPERTIES_KEY)) {
                        final Object propertiesObject = fieldMappingAsMap.get(PROPERTIES_KEY);
                        if (propertiesObject instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> properties = (Map<String, Object>) propertiesObject;
                            properties = removeDotsInFieldNamesFromProperties(properties);
                            fieldMappingAsMap = new HashMap<>(fieldMappingAsMap);
                            fieldMappingAsMap.put(PROPERTIES_KEY, properties);
                            fieldMapping = fieldMappingAsMap;
                        }
                    }
                }
                result.put(fieldName, fieldMapping);
            }
        }

        for (Map.Entry<String, ?> entry : fieldsWithDotsInFieldNames) {
            final String fieldName = entry.getKey();
            final Object fieldMapping = entry.getValue();
            final String[] splits = fieldName.split("\\.");
            assert splits.length > 1;

            Map<String, Object> innerMapping = result;
            for (int i = 0; i < splits.length - 1; ++i) {
                innerMapping = addObjectMapping(innerMapping, splits[i], fieldName);
            }
            if (innerMapping.containsKey(splits[splits.length - 1])) {
                throw new IllegalArgumentException("Mapping contains two definitions for field [" + fieldName + "]");
            }
            innerMapping.put(splits[splits.length - 1], fieldMapping);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> addObjectMapping(Map<String, Object> mapping, String name, String fullName) {
        Object fieldMapping = mapping.get(name);
        Map<String, Object> objectMapping;
        if (fieldMapping == null) {
            // no mapping for the current name yet, let's add it
            Map<String, Object> properties = new HashMap<>();
            objectMapping = new HashMap<>();
            objectMapping.put(PROPERTIES_KEY, properties);
            mapping.put(name, objectMapping);
            return properties;
        } else if (fieldMapping instanceof Map) {
            objectMapping = (Map<String, Object>) fieldMapping;
        } else {
            throw new IllegalArgumentException("Expected an object for mapping definition of field ["
                    + name + "] but got [" + fieldMapping + "]");
        }

        final Object type = objectMapping.get("type");
        if (type != null && type.equals("object") == false) {
            throw new IllegalArgumentException("Need to create an object mapping called [" + name
                    + "] for field [" + fullName + "] but this field already exists and is a [" + type + "]");
        }

        Object propertiesObject = objectMapping.get(PROPERTIES_KEY);
        Map<String, Object> properties;
        if (propertiesObject == null) {
            properties = new HashMap<>();
            objectMapping.put(PROPERTIES_KEY, properties);
        } else if (propertiesObject instanceof Map) {
            properties = (Map<String, Object>) propertiesObject;
        } else {
            throw new IllegalArgumentException("Expected an object for properties of object ["
                    + name + "] but got [" + propertiesObject + "]");
        }

        return properties;
    }
}
