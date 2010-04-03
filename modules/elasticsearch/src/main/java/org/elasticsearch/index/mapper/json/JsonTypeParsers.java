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
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.joda.FormatDateTimeFormatter;
import org.elasticsearch.util.joda.Joda;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.util.json.JacksonNodes.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonTypeParsers {

    public static void parseNumberField(JsonNumberFieldMapper.Builder builder, String name, ObjectNode numberNode, JsonTypeParser.ParserContext parserContext) {
        parseJsonField(builder, name, numberNode, parserContext);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = numberNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("precisionStep") || propName.equals("precision_step")) {
                builder.precisionStep(nodeIntegerValue(propNode));
            }
        }
    }

    public static void parseJsonField(JsonFieldMapper.Builder builder, String name, ObjectNode fieldNode, JsonTypeParser.ParserContext parserContext) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldNode.getFields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("indexName") || propName.equals("index_name")) {
                builder.indexName(propNode.getTextValue());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.getTextValue()));
            } else if (propName.equals("index")) {
                builder.index(parseIndex(name, propNode.getTextValue()));
            } else if (propName.equals("termVector") || propName.equals("term_vector")) {
                builder.termVector(parseTermVector(name, propNode.getTextValue()));
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
            } else if (propName.equals("omitNorms") || propName.equals("omit_norms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("omitTermFreqAndPositions") || propName.equals("omit_termFreq_and_positions")) {
                builder.omitTermFreqAndPositions(nodeBooleanValue(propNode));
            } else if (propName.equals("indexAnalyzer") || propName.equals("index_analyzer")) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("searchAnalyzer") || propName.equals("search_analyzer")) {
                builder.searchAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("analyzer")) {
                builder.indexAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
                builder.searchAnalyzer(parserContext.analysisService().analyzer(propNode.getTextValue()));
            } else if (propName.equals("includeInAll") || propName.equals("include_in_all")) {
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
