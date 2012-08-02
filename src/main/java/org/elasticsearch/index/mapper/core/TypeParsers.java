/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 *
 */
public class TypeParsers {

    public static void parseNumberField(NumberFieldMapper.Builder builder, String name, Map<String, Object> numberNode, Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, numberNode, parserContext);
        for (Map.Entry<String, Object> entry : numberNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("precision_step")) {
                builder.precisionStep(nodeIntegerValue(propNode));
            } else if (propName.equals("fuzzy_factor")) {
                builder.fuzzyFactor(propNode.toString());
            }
        }
    }

    public static void parseField(AbstractFieldMapper.Builder builder, String name, Map<String, Object> fieldNode, Mapper.TypeParser.ParserContext parserContext) {
        for (Map.Entry<String, Object> entry : fieldNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("index_name")) {
                builder.indexName(propNode.toString());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.toString()));
            } else if (propName.equals("index")) {
                builder.index(parseIndex(name, propNode.toString()));
            } else if (propName.equals("term_vector")) {
                builder.termVector(parseTermVector(name, propNode.toString()));
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
            } else if (propName.equals("omit_norms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("omit_term_freq_and_positions")) {
                builder.omitTermFreqAndPositions(nodeBooleanValue(propNode));
            } else if (propName.equals("analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.indexAnalyzer(analyzer);
                builder.searchAnalyzer(analyzer);
            } else if (propName.equals("index_analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.indexAnalyzer(analyzer);
            } else if (propName.equals("search_analyzer")) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.searchAnalyzer(analyzer);
            } else if (propName.equals("include_in_all")) {
                builder.includeInAll(nodeBooleanValue(propNode));
            }
        }
    }

    public static FormatDateTimeFormatter parseDateTimeFormatter(String fieldName, Object node) {
        return Joda.forPattern(node.toString());
    }

    public static Field.TermVector parseTermVector(String fieldName, String termVector) throws MapperParsingException {
        termVector = Strings.toUnderscoreCase(termVector);
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
        index = Strings.toUnderscoreCase(index);
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
            boolean value = nodeBooleanValue(store);
            if (value) {
                return Field.Store.YES;
            } else {
                return Field.Store.NO;
            }
        }
    }

    public static ContentPath.Type parsePathType(String name, String path) throws MapperParsingException {
        path = Strings.toUnderscoreCase(path);
        if ("just_name".equals(path)) {
            return ContentPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return ContentPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for object [" + name + "]");
        }
    }

}
