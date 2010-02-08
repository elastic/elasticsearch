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

import org.apache.lucene.document.Field;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.io.compression.GZIPCompressor;
import org.elasticsearch.util.io.compression.LzfCompressor;
import org.elasticsearch.util.io.compression.ZipCompressor;
import org.elasticsearch.util.joda.Joda;
import org.elasticsearch.util.json.Jackson;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonDocumentMapperParser implements DocumentMapperParser {

    private final ObjectMapper objectMapper = Jackson.newObjectMapper();

    private final AnalysisService analysisService;

    public JsonDocumentMapperParser(AnalysisService analysisService) {
        this.analysisService = analysisService;
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

        JsonDocumentMapper.Builder docBuilder = JsonMapperBuilders.doc(parseObject(type, rootObj));

        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = rootObj.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();

            if ("sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((ObjectNode) fieldNode));
            } else if ("idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((ObjectNode) fieldNode));
            } else if ("typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((ObjectNode) fieldNode));
            } else if ("uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((ObjectNode) fieldNode));
            } else if ("boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((ObjectNode) fieldNode));
            } else if ("indexAnalyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.getTextValue()));
            } else if ("searchAnalyzer".equals(fieldName)) {
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

        return docBuilder.build();
    }

    private JsonUidFieldMapper.Builder parseUidField(ObjectNode uidNode) {
        String name = uidNode.get("name") == null ? JsonUidFieldMapper.Defaults.NAME : uidNode.get("name").getTextValue();
        JsonUidFieldMapper.Builder builder = uid(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = uidNode.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();

            if ("indexName".equals(fieldName)) {
                builder.indexName(fieldNode.getTextValue());
            }
        }
        return builder;
    }

    private JsonBoostFieldMapper.Builder parseBoostField(ObjectNode boostNode) {
        String name = boostNode.get("name") == null ? JsonBoostFieldMapper.Defaults.NAME : boostNode.get("name").getTextValue();
        JsonBoostFieldMapper.Builder builder = boost(name);
        parseNumberField(builder, name, boostNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = boostNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getNumberValue().floatValue());
            }
        }
        return builder;
    }

    private JsonTypeFieldMapper.Builder parseTypeField(ObjectNode typeNode) {
        String name = typeNode.get("name") == null ? JsonTypeFieldMapper.Defaults.NAME : typeNode.get("name").getTextValue();
        JsonTypeFieldMapper.Builder builder = type(name);
        parseJsonField(builder, name, typeNode);
        return builder;
    }


    private JsonIdFieldMapper.Builder parseIdField(ObjectNode idNode) {
        String name = idNode.get("name") == null ? JsonIdFieldMapper.Defaults.NAME : idNode.get("name").getTextValue();
        JsonIdFieldMapper.Builder builder = id(name);
        parseJsonField(builder, name, idNode);
        return builder;
    }

    private JsonSourceFieldMapper.Builder parseSourceField(ObjectNode sourceNode) {
        String name = sourceNode.get("name") == null ? JsonSourceFieldMapper.Defaults.NAME : sourceNode.get("name").getTextValue();
        JsonSourceFieldMapper.Builder builder = source(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = sourceNode.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("compressionThreshold")) {
                builder.compressionThreshold(fieldNode.getNumberValue().intValue());
            } else if (fieldName.equals("compressionType")) {
                String compressionType = fieldNode.getTextValue();
                if ("zip".equals(compressionType)) {
                    builder.compressor(new ZipCompressor());
                } else if ("gzip".equals(compressionType)) {
                    builder.compressor(new GZIPCompressor());
                } else if ("lzf".equals(compressionType)) {
                    builder.compressor(new LzfCompressor());
                } else {
                    throw new MapperParsingException("No compressor registed under [" + compressionType + "]");
                }
            }
        }
        return builder;
    }

    private JsonObjectMapper.Builder parseObject(String name, ObjectNode node) {
        JsonObjectMapper.Builder builder = object(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = node.getFields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("dynamic")) {
                builder.dynamic(fieldNode.getBooleanValue());
            } else if (fieldName.equals("type")) {
                String type = fieldNode.getTextValue();
                if (!type.equals("object")) {
                    throw new MapperParsingException("Trying to parse an object but has a different type [" + type + "] for [" + name + "]");
                }
            } else if (fieldName.equals("dateFormats")) {
                List<DateTimeFormatter> dateTimeFormatters = newArrayList();
                if (fieldNode.isArray()) {
                    for (JsonNode node1 : (ArrayNode) fieldNode) {
                        dateTimeFormatters.add(parseDateTimeFormatter(fieldName, node1));
                    }
                } else if ("none".equals(fieldNode.getValueAsText())) {
                    dateTimeFormatters = null;
                } else {
                    dateTimeFormatters.add(parseDateTimeFormatter(fieldName, fieldNode));
                }
                if (dateTimeFormatters == null) {
                    builder.noDateTimeFormatter();
                } else {
                    builder.dateTimeFormatter(dateTimeFormatters);
                }
            } else if (fieldName.equals("enabled")) {
                builder.enabled(fieldNode.getBooleanValue());
            } else if (fieldName.equals("pathType")) {
                builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
            } else if (fieldName.equals("properties")) {
                parseProperties(builder, (ObjectNode) fieldNode);
            }
        }
        return builder;
    }

    private JsonPath.Type parsePathType(String name, String path) throws MapperParsingException {
        if ("justName".equals(path)) {
            return JsonPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return JsonPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for objet [" + name + "]");
        }
    }

    private void parseProperties(JsonObjectMapper.Builder objBuilder, ObjectNode propsNode) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = propsNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();

            String type;
            JsonNode typeNode = propNode.get("type");
            if (typeNode != null) {
                type = typeNode.getTextValue();
            } else {
                // lets see if we can derive this...
                if (propNode.isObject() && propNode.get("properties") != null) {
                    type = "object";
                } else {
                    throw new MapperParsingException("No type specified for property [" + propName + "]");
                }
            }
            if (type.equals("string")) {
                objBuilder.add(parseString(propName, (ObjectNode) propNode));
            } else if (type.equals("date")) {
                objBuilder.add(parseDate(propName, (ObjectNode) propNode));
            } else if (type.equals("integer")) {
                objBuilder.add(parseInteger(propName, (ObjectNode) propNode));
            } else if (type.equals("long")) {
                objBuilder.add(parseLong(propName, (ObjectNode) propNode));
            } else if (type.equals("float")) {
                objBuilder.add(parseFloat(propName, (ObjectNode) propNode));
            } else if (type.equals("double")) {
                objBuilder.add(parseDouble(propName, (ObjectNode) propNode));
            } else if (type.equals("boolean")) {
                objBuilder.add(parseBoolean(propName, (ObjectNode) propNode));
            } else if (type.equals("object")) {
                objBuilder.add(parseObject(propName, (ObjectNode) propNode));
            } else if (type.equals("binary")) {
                objBuilder.add(parseBinary(propName, (ObjectNode) propNode));
            }
        }
    }

    private JsonDateFieldMapper.Builder parseDate(String name, ObjectNode dateNode) {
        JsonDateFieldMapper.Builder builder = dateField(name);
        parseNumberField(builder, name, dateNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = dateNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getValueAsText());
            } else if (propName.equals("format")) {
                builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
            }
        }
        return builder;
    }


    private JsonIntegerFieldMapper.Builder parseInteger(String name, ObjectNode integerNode) {
        JsonIntegerFieldMapper.Builder builder = integerField(name);
        parseNumberField(builder, name, integerNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = integerNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getNumberValue().intValue());
            }
        }
        return builder;
    }

    private JsonLongFieldMapper.Builder parseLong(String name, ObjectNode longNode) {
        JsonLongFieldMapper.Builder builder = longField(name);
        parseNumberField(builder, name, longNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = longNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getNumberValue().longValue());
            }
        }
        return builder;
    }

    private JsonFloatFieldMapper.Builder parseFloat(String name, ObjectNode floatNode) {
        JsonFloatFieldMapper.Builder builder = floatField(name);
        parseNumberField(builder, name, floatNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = floatNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getNumberValue().floatValue());
            }
        }
        return builder;
    }

    private JsonDoubleFieldMapper.Builder parseDouble(String name, ObjectNode doubleNode) {
        JsonDoubleFieldMapper.Builder builder = doubleField(name);
        parseNumberField(builder, name, doubleNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = doubleNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getNumberValue().doubleValue());
            }
        }
        return builder;
    }

    private JsonStringFieldMapper.Builder parseString(String name, ObjectNode stringNode) {
        JsonStringFieldMapper.Builder builder = stringField(name);
        parseJsonField(builder, name, stringNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = stringNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getValueAsText());
            }
        }
        return builder;
    }

    private JsonBinaryFieldMapper.Builder parseBinary(String name, ObjectNode binaryNode) {
        JsonBinaryFieldMapper.Builder builder = binaryField(name);
        parseJsonField(builder, name, binaryNode);
        return builder;
    }

    private JsonBooleanFieldMapper.Builder parseBoolean(String name, ObjectNode booleanNode) {
        JsonBooleanFieldMapper.Builder builder = booleanField(name);
        parseJsonField(builder, name, booleanNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = booleanNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.getBooleanValue());
            }
        }
        return builder;
    }

    private void parseNumberField(JsonNumberFieldMapper.Builder builder, String name, ObjectNode numberNode) {
        parseJsonField(builder, name, numberNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = numberNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("precisionStep")) {
                builder.precisionStep(propNode.getNumberValue().intValue());
            }
        }
    }

    private void parseJsonField(JsonFieldMapper.Builder builder, String name, ObjectNode fieldNode) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("indexName")) {
                builder.indexName(propNode.getValueAsText());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.getTextValue()));
            } else if (propName.equals("index")) {
                builder.index(parseIndex(name, propNode.getTextValue()));
            } else if (propName.equals("termVector")) {
                builder.termVector(parseTermVector(name, propNode.getTextValue()));
            } else if (propName.equals("boost")) {
                builder.boost(propNode.getNumberValue().floatValue());
            } else if (propName.equals("omitNorms")) {
                builder.omitNorms(propNode.getBooleanValue());
            } else if (propName.equals("omitTermFreqAndPositions")) {
                builder.omitTermFreqAndPositions(propNode.getBooleanValue());
            } else if (propName.equals("indexAnalyzer")) {
                builder.indexAnalyzer(analysisService.analyzer(propNode.getTextValue()));
            } else if (propName.equals("searchAnalyzer")) {
                builder.searchAnalyzer(analysisService.analyzer(propNode.getTextValue()));
            } else if (propName.equals("analyzer")) {
                builder.indexAnalyzer(analysisService.analyzer(propNode.getTextValue()));
                builder.searchAnalyzer(analysisService.analyzer(propNode.getTextValue()));
            }
        }
    }

    private DateTimeFormatter parseDateTimeFormatter(String fieldName, JsonNode node) {
        if (node.isTextual()) {
            return Joda.forPattern(node.getTextValue()).withZone(DateTimeZone.UTC);
        } else {
            // TODO support more complex configuration...
            throw new MapperParsingException("Wrong node to use to parse date formatters [" + fieldName + "]");
        }
    }

    private Field.TermVector parseTermVector(String fieldName, String termVector) throws MapperParsingException {
        if ("no".equals(termVector)) {
            return Field.TermVector.NO;
        } else if ("yes".equals(termVector)) {
            return Field.TermVector.YES;
        } else if ("with_offsets".equals(termVector)) {
            return Field.TermVector.WITH_OFFSETS;
        } else if ("with_positions".equals(termVector)) {
            return Field.TermVector.WITH_POSITIONS;
        } else if ("with_positions_offsets".equals(termVector)) {
            return Field.TermVector.WITH_POSITIONS_OFFSETS;
        } else {
            throw new MapperParsingException("Wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    private Field.Index parseIndex(String fieldName, String index) throws MapperParsingException {
        if ("no".equals(index)) {
            return Field.Index.NO;
        } else if ("not_analyzed".equals(index)) {
            return Field.Index.NOT_ANALYZED;
        } else if ("analyzed".equals(index)) {
            return Field.Index.ANALYZED;
        } else {
            throw new MapperParsingException("Wrong value for index [" + index + "] for field [" + fieldName + "]");
        }
    }

    private Field.Store parseStore(String fieldName, String store) throws MapperParsingException {
        if ("no".equals(store)) {
            return Field.Store.NO;
        } else if ("yes".equals(store)) {
            return Field.Store.YES;
        } else {
            throw new MapperParsingException("Wrong value for store [" + store + "] for field [" + fieldName + "]");
        }
    }
}
