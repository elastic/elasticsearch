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
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.joda.FormatDateTimeFormatter;
import org.elasticsearch.util.joda.Joda;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.util.json.JacksonNodes.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonTypeParsers {

    public static class JsonObjectTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode objectNode = (ObjectNode) node;
            JsonObjectMapper.Builder builder = object(name);
            for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = objectNode.getFields(); fieldsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String fieldName = entry.getKey();
                JsonNode fieldNode = entry.getValue();
                if (fieldName.equals("dynamic")) {
                    builder.dynamic(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("type")) {
                    String type = fieldNode.getTextValue();
                    if (!type.equals("object")) {
                        throw new MapperParsingException("Trying to parse an object but has a different type [" + type + "] for [" + name + "]");
                    }
                } else if (fieldName.equals("dateFormats")) {
                    List<FormatDateTimeFormatter> dateTimeFormatters = newArrayList();
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
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("pathType")) {
                    builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
                } else if (fieldName.equals("properties")) {
                    parseProperties(builder, (ObjectNode) fieldNode, parserContext);
                } else if (fieldName.equals("includeInAll")) {
                    builder.includeInAll(nodeBooleanValue(fieldNode));
                }
            }
            return builder;
        }

        private void parseProperties(JsonObjectMapper.Builder objBuilder, ObjectNode propsNode, JsonTypeParser.ParserContext parserContext) {
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
                        type = JsonObjectMapper.JSON_TYPE;
                    } else if (propNode.isObject() && propNode.get("fields") != null) {
                        type = JsonMultiFieldMapper.JSON_TYPE;
                    } else {
                        throw new MapperParsingException("No type specified for property [" + propName + "]");
                    }
                }

                JsonTypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + propName + "]");
                }
                objBuilder.add(typeParser.parse(propName, propNode, parserContext));
            }
        }
    }

    public static class JsonMultiFieldTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode multiFieldNode = (ObjectNode) node;
            JsonMultiFieldMapper.Builder builder = multiField(name);
            for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = multiFieldNode.getFields(); fieldsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String fieldName = entry.getKey();
                JsonNode fieldNode = entry.getValue();
                if (fieldName.equals("pathType")) {
                    builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
                } else if (fieldName.equals("fields")) {
                    ObjectNode fieldsNode = (ObjectNode) fieldNode;
                    for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldsNode.getFields(); propsIt.hasNext();) {
                        Map.Entry<String, JsonNode> entry1 = propsIt.next();
                        String propName = entry1.getKey();
                        JsonNode propNode = entry1.getValue();

                        String type;
                        JsonNode typeNode = propNode.get("type");
                        if (typeNode != null) {
                            type = typeNode.getTextValue();
                        } else {
                            throw new MapperParsingException("No type specified for property [" + propName + "]");
                        }

                        JsonTypeParser typeParser = parserContext.typeParser(type);
                        if (typeParser == null) {
                            throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                        }
                        builder.add(typeParser.parse(propName, propNode, parserContext));
                    }
                }
            }
            return builder;
        }
    }

    public static class JsonDateTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode dateNode = (ObjectNode) node;
            JsonDateFieldMapper.Builder builder = dateField(name);
            parseNumberField(builder, name, dateNode, parserContext);
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
    }

    public static class JsonShortTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode shortNode = (ObjectNode) node;
            JsonShortFieldMapper.Builder builder = shortField(name);
            parseNumberField(builder, name, shortNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = shortNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeShortValue(propNode));
                }
            }
            return builder;
        }
    }

    public static class JsonIntegerTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode integerNode = (ObjectNode) node;
            JsonIntegerFieldMapper.Builder builder = integerField(name);
            parseNumberField(builder, name, integerNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = integerNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeIntegerValue(propNode));
                }
            }
            return builder;
        }
    }

    public static class JsonLongTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode longNode = (ObjectNode) node;
            JsonLongFieldMapper.Builder builder = longField(name);
            parseNumberField(builder, name, longNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = longNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeLongValue(propNode));
                }
            }
            return builder;
        }
    }

    public static class JsonFloatTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode floatNode = (ObjectNode) node;
            JsonFloatFieldMapper.Builder builder = floatField(name);
            parseNumberField(builder, name, floatNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = floatNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeFloatValue(propNode));
                }
            }
            return builder;
        }
    }

    public static class JsonDoubleTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode doubleNode = (ObjectNode) node;
            JsonDoubleFieldMapper.Builder builder = doubleField(name);
            parseNumberField(builder, name, doubleNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = doubleNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeDoubleValue(propNode));
                }
            }
            return builder;
        }
    }

    public static class JsonStringTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode stringNode = (ObjectNode) node;
            JsonStringFieldMapper.Builder builder = stringField(name);
            parseJsonField(builder, name, stringNode, parserContext);
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
    }

    public static class JsonBinaryTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode binaryNode = (ObjectNode) node;
            JsonBinaryFieldMapper.Builder builder = binaryField(name);
            parseJsonField(builder, name, binaryNode, parserContext);
            return builder;
        }
    }

    public static class JsonBooleanTypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode booleanNode = (ObjectNode) node;
            JsonBooleanFieldMapper.Builder builder = booleanField(name);
            parseJsonField(builder, name, booleanNode, parserContext);
            for (Iterator<Map.Entry<String, JsonNode>> propsIt = booleanNode.getFields(); propsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = propsIt.next();
                String propName = entry.getKey();
                JsonNode propNode = entry.getValue();
                if (propName.equals("nullValue")) {
                    builder.nullValue(nodeBooleanValue(propNode));
                }
            }
            return builder;
        }
    }

    public static void parseNumberField(JsonNumberFieldMapper.Builder builder, String name, ObjectNode numberNode, JsonTypeParser.ParserContext parserContext) {
        parseJsonField(builder, name, numberNode, parserContext);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = numberNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("precisionStep")) {
                builder.precisionStep(nodeIntegerValue(propNode));
            }
        }
    }

    public static void parseJsonField(JsonFieldMapper.Builder builder, String name, ObjectNode fieldNode, JsonTypeParser.ParserContext parserContext) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("indexName")) {
                builder.indexName(propNode.getTextValue());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.getTextValue()));
            } else if (propName.equals("index")) {
                builder.index(parseIndex(name, propNode.getTextValue()));
            } else if (propName.equals("termVector")) {
                builder.termVector(parseTermVector(name, propNode.getTextValue()));
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
            } else if (propName.equals("omitNorms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("omitTermFreqAndPositions")) {
                builder.omitTermFreqAndPositions(nodeBooleanValue(propNode));
            } else if (propName.equals("indexAnalyzer")) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("searchAnalyzer")) {
                builder.searchAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("analyzer")) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
                builder.searchAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("includeInAll")) {
                builder.includeInAll(nodeBooleanValue(propNode));
            }
        }
    }

    public static FormatDateTimeFormatter parseDateTimeFormatter(String fieldName, JsonNode node) {
        if (node.isTextual()) {
            return Joda.forPattern(node.getTextValue());
        } else {
            // TODO support more complex configuration...
            throw new MapperParsingException("Wrong node to use to parse date formatters [" + fieldName + "]");
        }
    }

    public static Field.TermVector parseTermVector(String fieldName, String termVector) throws MapperParsingException {
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

    public static Field.Index parseIndex(String fieldName, String index) throws MapperParsingException {
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

    public static Field.Store parseStore(String fieldName, String store) throws MapperParsingException {
        if ("no".equals(store)) {
            return Field.Store.NO;
        } else if ("yes".equals(store)) {
            return Field.Store.YES;
        } else {
            throw new MapperParsingException("Wrong value for store [" + store + "] for field [" + fieldName + "]");
        }
    }

    public static JsonPath.Type parsePathType(String name, String path) throws MapperParsingException {
        if ("justName".equals(path) || "just_name".equals(path)) {
            return JsonPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return JsonPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for objet [" + name + "]");
        }
    }

}
