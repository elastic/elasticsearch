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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.mapper.MapperBuilders.doc;

public class DocumentMapperParser {

    final MapperService mapperService;
    final AnalysisService analysisService;
    private static final ESLogger logger = Loggers.getLogger(DocumentMapperParser.class);
    private final SimilarityService similarityService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Version indexVersionCreated;
    private final ParseFieldMatcher parseFieldMatcher;

    private final Map<String, Mapper.TypeParser> typeParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;

    public DocumentMapperParser(IndexSettings indexSettings, MapperService mapperService, AnalysisService analysisService,
                                SimilarityService similarityService, MapperRegistry mapperRegistry) {
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings.getSettings());
        this.mapperService = mapperService;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.typeParsers = mapperRegistry.getMapperParsers();
        this.rootTypeParsers = mapperRegistry.getMetadataMapperParsers();
        indexVersionCreated = indexSettings.getIndexVersionCreated();
    }

    public Mapper.TypeParser.ParserContext parserContext(String type) {
        return new Mapper.TypeParser.ParserContext(type, analysisService, similarityService::getSimilarity, mapperService, typeParsers::get, indexVersionCreated, parseFieldMatcher);
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
    private DocumentMapper parse(String type, Map<String, Object> mapping, String defaultSource) throws MapperParsingException {
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
        DocumentMapper.Builder docBuilder = doc((RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext), mapperService);
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
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

    public static void checkNoRemainingFields(String fieldName, Map<String, Object> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(fieldNodeMap, indexVersionCreated, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<String, Object> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            if (indexVersionCreated.onOrAfter(Version.V_2_0_0_beta1)) {
                throw new MapperParsingException(message + getRemainingFields(fieldNodeMap));
            } else {
                logger.debug(message + "{}", getRemainingFields(fieldNodeMap));
            }
        }
    }

    private static String getRemainingFields(Map<String, ?> map) {
        StringBuilder remainingFields = new StringBuilder();
        for (String key : map.keySet()) {
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
        String rootName = root.keySet().iterator().next();
        Tuple<String, Map<String, Object>> mapping;
        if (type == null || type.equals(rootName)) {
            mapping = new Tuple<>(rootName, (Map<String, Object>) root.get(rootName));
        } else {
            mapping = new Tuple<>(type, root);
        }
        return mapping;
    }
}
