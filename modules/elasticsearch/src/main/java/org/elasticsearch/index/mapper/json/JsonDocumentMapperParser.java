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

package org.elasticsearch.index.mapper.json;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.gcommon.collect.ImmutableMap;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;
import static org.elasticsearch.util.json.JacksonNodes.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonDocumentMapperParser implements DocumentMapperParser {

    private final ObjectMapper objectMapper = Jackson.newObjectMapper();

    private final AnalysisService analysisService;

    private final JsonObjectMapper.TypeParser rootObjectTypeParser = new JsonObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();

    private volatile ImmutableMap<String, JsonTypeParser> typeParsers;

    public JsonDocumentMapperParser(AnalysisService analysisService) {
        this.analysisService = analysisService;
        typeParsers = new MapBuilder<String, JsonTypeParser>()
                .put(JsonShortFieldMapper.JSON_TYPE, new JsonShortFieldMapper.TypeParser())
                .put(JsonIntegerFieldMapper.JSON_TYPE, new JsonIntegerFieldMapper.TypeParser())
                .put(JsonLongFieldMapper.JSON_TYPE, new JsonLongFieldMapper.TypeParser())
                .put(JsonFloatFieldMapper.JSON_TYPE, new JsonFloatFieldMapper.TypeParser())
                .put(JsonDoubleFieldMapper.JSON_TYPE, new JsonDoubleFieldMapper.TypeParser())
                .put(JsonBooleanFieldMapper.JSON_TYPE, new JsonBooleanFieldMapper.TypeParser())
                .put(JsonBinaryFieldMapper.JSON_TYPE, new JsonBinaryFieldMapper.TypeParser())
                .put(JsonDateFieldMapper.JSON_TYPE, new JsonDateFieldMapper.TypeParser())
                .put(JsonStringFieldMapper.JSON_TYPE, new JsonStringFieldMapper.TypeParser())
                .put(JsonObjectMapper.JSON_TYPE, new JsonObjectMapper.TypeParser())
                .put(JsonMultiFieldMapper.JSON_TYPE, new JsonMultiFieldMapper.TypeParser())
                .immutableMap();
    }

    public void putTypeParser(String type, JsonTypeParser typeParser) {
        synchronized (typeParsersMutex) {
            typeParsers = new MapBuilder<String, JsonTypeParser>()
                    .putAll(typeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    @Override public DocumentMapper parse(String source) throws MapperParsingException {
        return parse(null, source);
    }

    @Override public DocumentMapper parse(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try {
            root = objectMapper.readValue(new FastStringReader(source), Map.class);
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse json mapping definition", e);
        }
        String rootName = root.keySet().iterator().next();
        Map<String, Object> rootObj;
        if (type == null) {
            // we have no type, we assume the first node is the type
            rootObj = (Map<String, Object>) root.get(rootName);
            type = rootName;
        } else {
            // we have a type, check if the top level one is the type as well
            // if it is, then the root is that node, if not then the root is the master node
            if (type.equals(rootName)) {
                Object tmpNode = root.get(type);
                if (!(tmpNode instanceof Map)) {
                    throw new MapperParsingException("Expected root node name [" + rootName + "] to be of object type, but its not");
                }
                rootObj = (Map<String, Object>) tmpNode;
            } else {
                rootObj = root;
            }
        }

        JsonTypeParser.ParserContext parserContext = new JsonTypeParser.ParserContext(rootObj, analysisService, typeParsers);

        JsonDocumentMapper.Builder docBuilder = doc((JsonObjectMapper.Builder) rootObjectTypeParser.parse(type, rootObj, parserContext));

        for (Map.Entry<String, Object> entry : rootObj.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if (JsonSourceFieldMapper.JSON_TYPE.equals(fieldName) || "sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((Map<String, Object>) fieldNode, parserContext));
            } else if (JsonIdFieldMapper.JSON_TYPE.equals(fieldName) || "idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((Map<String, Object>) fieldNode, parserContext));
            } else if (JsonTypeFieldMapper.JSON_TYPE.equals(fieldName) || "typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((Map<String, Object>) fieldNode, parserContext));
            } else if (JsonUidFieldMapper.JSON_TYPE.equals(fieldName) || "uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((Map<String, Object>) fieldNode, parserContext));
            } else if (JsonBoostFieldMapper.JSON_TYPE.equals(fieldName) || "boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((Map<String, Object>) fieldNode, parserContext));
            } else if (JsonAllFieldMapper.JSON_TYPE.equals(fieldName) || "allField".equals(fieldName)) {
                docBuilder.allField(parseAllField((Map<String, Object>) fieldNode, parserContext));
            } else if ("index_analyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.toString()));
            } else if ("search_analyzer".equals(fieldName)) {
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.toString()));
            } else if ("analyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.toString()));
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.toString()));
            }
        }

        if (!docBuilder.hasIndexAnalyzer()) {
            docBuilder.indexAnalyzer(analysisService.defaultIndexAnalyzer());
        }
        if (!docBuilder.hasSearchAnalyzer()) {
            docBuilder.searchAnalyzer(analysisService.defaultSearchAnalyzer());
        }

        docBuilder.mappingSource(source);

        JsonDocumentMapper documentMapper = docBuilder.build();
        // update the source with the generated one
        documentMapper.mappingSource(documentMapper.buildSource());
        return documentMapper;
    }

    private JsonUidFieldMapper.Builder parseUidField(Map<String, Object> uidNode, JsonTypeParser.ParserContext parserContext) {
        JsonUidFieldMapper.Builder builder = uid();
        return builder;
    }

    private JsonBoostFieldMapper.Builder parseBoostField(Map<String, Object> boostNode, JsonTypeParser.ParserContext parserContext) {
        String name = boostNode.get("name") == null ? JsonBoostFieldMapper.Defaults.NAME : boostNode.get("name").toString();
        JsonBoostFieldMapper.Builder builder = boost(name);
        parseNumberField(builder, name, boostNode, parserContext);
        for (Map.Entry<String, Object> entry : boostNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("null_value")) {
                builder.nullValue(nodeFloatValue(propNode));
            }
        }
        return builder;
    }

    private JsonTypeFieldMapper.Builder parseTypeField(Map<String, Object> typeNode, JsonTypeParser.ParserContext parserContext) {
        JsonTypeFieldMapper.Builder builder = type();
        parseJsonField(builder, builder.name, typeNode, parserContext);
        return builder;
    }


    private JsonIdFieldMapper.Builder parseIdField(Map<String, Object> idNode, JsonTypeParser.ParserContext parserContext) {
        JsonIdFieldMapper.Builder builder = id();
        parseJsonField(builder, builder.name, idNode, parserContext);
        return builder;
    }

    private JsonAllFieldMapper.Builder parseAllField(Map<String, Object> allNode, JsonTypeParser.ParserContext parserContext) {
        JsonAllFieldMapper.Builder builder = all();
        parseJsonField(builder, builder.name, allNode, parserContext);
        for (Map.Entry<String, Object> entry : allNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }

    private JsonSourceFieldMapper.Builder parseSourceField(Map<String, Object> sourceNode, JsonTypeParser.ParserContext parserContext) {
        JsonSourceFieldMapper.Builder builder = source();

        for (Map.Entry<String, Object> entry : sourceNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }
}
