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

import org.elasticsearch.util.gcommon.collect.ImmutableMap;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;

import java.io.IOException;
import java.util.Iterator;
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
        JsonNode root;
        try {
            root = objectMapper.readValue(new FastStringReader(source), JsonNode.class);
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse json mapping definition", e);
        }
        String rootName = root.getFieldNames().next();
        ObjectNode rootObj;
        if (type == null) {
            // we have no type, we assume the first node is the type
            rootObj = (ObjectNode) root.get(rootName);
            type = rootName;
        } else {
            // we have a type, check if the top level one is the type as well
            // if it is, then the root is that node, if not then the root is the master node
            if (type.equals(rootName)) {
                JsonNode tmpNode = root.get(type);
                if (!tmpNode.isObject()) {
                    throw new MapperParsingException("Expected root node name [" + rootName + "] to be of json object type, but its not");
                }
                rootObj = (ObjectNode) tmpNode;
            } else {
                rootObj = (ObjectNode) root;
            }
        }

        JsonTypeParser.ParserContext parserContext = new JsonTypeParser.ParserContext(rootObj, analysisService, typeParsers);

        JsonDocumentMapper.Builder docBuilder = doc((JsonObjectMapper.Builder) rootObjectTypeParser.parse(type, rootObj, parserContext));

        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = rootObj.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            JsonNode fieldNode = entry.getValue();

            if (JsonSourceFieldMapper.JSON_TYPE.equals(fieldName) || "sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((ObjectNode) fieldNode, parserContext));
            } else if (JsonIdFieldMapper.JSON_TYPE.equals(fieldName) || "idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((ObjectNode) fieldNode, parserContext));
            } else if (JsonTypeFieldMapper.JSON_TYPE.equals(fieldName) || "typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((ObjectNode) fieldNode, parserContext));
            } else if (JsonUidFieldMapper.JSON_TYPE.equals(fieldName) || "uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((ObjectNode) fieldNode, parserContext));
            } else if (JsonBoostFieldMapper.JSON_TYPE.equals(fieldName) || "boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((ObjectNode) fieldNode, parserContext));
            } else if (JsonAllFieldMapper.JSON_TYPE.equals(fieldName) || "allField".equals(fieldName)) {
                docBuilder.allField(parseAllField((ObjectNode) fieldNode, parserContext));
            } else if ("index_analyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.getTextValue()));
            } else if ("search_analyzer".equals(fieldName)) {
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.getTextValue()));
            } else if ("analyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.getTextValue()));
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.getTextValue()));
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

    private JsonUidFieldMapper.Builder parseUidField(ObjectNode uidNode, JsonTypeParser.ParserContext parserContext) {
//        String name = uidNode.get("name") == null ? JsonUidFieldMapper.Defaults.NAME : uidNode.get("name").getTextValue();
        JsonUidFieldMapper.Builder builder = uid();
//        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = uidNode.getFields(); fieldsIt.hasNext();) {
//            Map.Entry<String, JsonNode> entry = fieldsIt.next();
//            String fieldName = entry.getKey();
//            JsonNode fieldNode = entry.getValue();
//
//            if ("indexName".equals(fieldName)) {
//                builder.indexName(fieldNode.getTextValue());
//            }
//        }
        return builder;
    }

    private JsonBoostFieldMapper.Builder parseBoostField(ObjectNode boostNode, JsonTypeParser.ParserContext parserContext) {
        String name = boostNode.get("name") == null ? JsonBoostFieldMapper.Defaults.NAME : boostNode.get("name").getTextValue();
        JsonBoostFieldMapper.Builder builder = boost(name);
        parseNumberField(builder, name, boostNode, parserContext);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = boostNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = Strings.toUnderscoreCase(entry.getKey());
            JsonNode propNode = entry.getValue();
            if (propName.equals("null_value")) {
                builder.nullValue(nodeFloatValue(propNode));
            }
        }
        return builder;
    }

    private JsonTypeFieldMapper.Builder parseTypeField(ObjectNode typeNode, JsonTypeParser.ParserContext parserContext) {
//        String name = typeNode.get("name") == null ? JsonTypeFieldMapper.Defaults.NAME : typeNode.get("name").getTextValue();
        JsonTypeFieldMapper.Builder builder = type();
        parseJsonField(builder, builder.name, typeNode, parserContext);
        return builder;
    }


    private JsonIdFieldMapper.Builder parseIdField(ObjectNode idNode, JsonTypeParser.ParserContext parserContext) {
//        String name = idNode.get("name") == null ? JsonIdFieldMapper.Defaults.NAME : idNode.get("name").getTextValue();
        JsonIdFieldMapper.Builder builder = id();
        parseJsonField(builder, builder.name, idNode, parserContext);
        return builder;
    }

    private JsonAllFieldMapper.Builder parseAllField(ObjectNode allNode, JsonTypeParser.ParserContext parserContext) {
//        String name = idNode.get("name") == null ? JsonIdFieldMapper.Defaults.NAME : idNode.get("name").getTextValue();
        JsonAllFieldMapper.Builder builder = all();
        parseJsonField(builder, builder.name, allNode, parserContext);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = allNode.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }

    private JsonSourceFieldMapper.Builder parseSourceField(ObjectNode sourceNode, JsonTypeParser.ParserContext parserContext) {
//        String name = sourceNode.get("name") == null ? JsonSourceFieldMapper.Defaults.NAME : sourceNode.get("name").getTextValue();
        JsonSourceFieldMapper.Builder builder = source();
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = sourceNode.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
//            if (fieldName.equals("compressionThreshold")) {
//                builder.compressionThreshold(nodeIn...);
//            } else if (fieldName.equals("compressionType")) {
//                String compressionType = fieldNode.getTextValue();
//                if ("zip".equals(compressionType)) {
//                    builder.compressor(new ZipCompressor());
//                } else if ("gzip".equals(compressionType)) {
//                    builder.compressor(new GZIPCompressor());
//                } else if ("lzf".equals(compressionType)) {
//                    builder.compressor(new LzfCompressor());
//                } else {
//                    throw new MapperParsingException("No compressor registed under [" + compressionType + "]");
//                }
//            }
        }
        return builder;
    }
}
