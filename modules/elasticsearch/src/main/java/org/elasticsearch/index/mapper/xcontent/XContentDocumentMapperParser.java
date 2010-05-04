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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;
import static org.elasticsearch.util.xcontent.support.XContentMapValues.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentDocumentMapperParser implements DocumentMapperParser {

    private final AnalysisService analysisService;

    private final XContentObjectMapper.TypeParser rootObjectTypeParser = new XContentObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();

    private volatile ImmutableMap<String, XContentTypeParser> typeParsers;

    public XContentDocumentMapperParser(AnalysisService analysisService) {
        this.analysisService = analysisService;
        typeParsers = new MapBuilder<String, XContentTypeParser>()
                .put(XContentShortFieldMapper.CONTENT_TYPE, new XContentShortFieldMapper.TypeParser())
                .put(XContentIntegerFieldMapper.CONTENT_TYPE, new XContentIntegerFieldMapper.TypeParser())
                .put(XContentLongFieldMapper.CONTENT_TYPE, new XContentLongFieldMapper.TypeParser())
                .put(XContentFloatFieldMapper.CONTENT_TYPE, new XContentFloatFieldMapper.TypeParser())
                .put(XContentDoubleFieldMapper.CONTENT_TYPE, new XContentDoubleFieldMapper.TypeParser())
                .put(XContentBooleanFieldMapper.CONTENT_TYPE, new XContentBooleanFieldMapper.TypeParser())
                .put(XContentBinaryFieldMapper.CONTENT_TYPE, new XContentBinaryFieldMapper.TypeParser())
                .put(XContentDateFieldMapper.CONTENT_TYPE, new XContentDateFieldMapper.TypeParser())
                .put(XContentStringFieldMapper.CONTENT_TYPE, new XContentStringFieldMapper.TypeParser())
                .put(XContentObjectMapper.CONTENT_TYPE, new XContentObjectMapper.TypeParser())
                .put(XContentMultiFieldMapper.CONTENT_TYPE, new XContentMultiFieldMapper.TypeParser())
                .immutableMap();
    }

    public void putTypeParser(String type, XContentTypeParser typeParser) {
        synchronized (typeParsersMutex) {
            typeParsers = new MapBuilder<String, XContentTypeParser>()
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
        XContentParser xContentParser = null;
        try {
            xContentParser = XContentFactory.xContent(source).createParser(source);
            root = xContentParser.map();
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse mapping definition", e);
        } finally {
            if (xContentParser != null) {
                xContentParser.close();
            }
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

        XContentTypeParser.ParserContext parserContext = new XContentTypeParser.ParserContext(rootObj, analysisService, typeParsers);

        XContentDocumentMapper.Builder docBuilder = doc((XContentObjectMapper.Builder) rootObjectTypeParser.parse(type, rootObj, parserContext));

        for (Map.Entry<String, Object> entry : rootObj.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if (XContentSourceFieldMapper.CONTENT_TYPE.equals(fieldName) || "sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentIdFieldMapper.CONTENT_TYPE.equals(fieldName) || "idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentTypeFieldMapper.CONTENT_TYPE.equals(fieldName) || "typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentUidFieldMapper.CONTENT_TYPE.equals(fieldName) || "uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentBoostFieldMapper.CONTENT_TYPE.equals(fieldName) || "boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentAllFieldMapper.CONTENT_TYPE.equals(fieldName) || "allField".equals(fieldName)) {
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

        XContentDocumentMapper documentMapper = docBuilder.build();
        // update the source with the generated one
        documentMapper.mappingSource(documentMapper.buildSource());
        return documentMapper;
    }

    private XContentUidFieldMapper.Builder parseUidField(Map<String, Object> uidNode, XContentTypeParser.ParserContext parserContext) {
        XContentUidFieldMapper.Builder builder = uid();
        return builder;
    }

    private XContentBoostFieldMapper.Builder parseBoostField(Map<String, Object> boostNode, XContentTypeParser.ParserContext parserContext) {
        String name = boostNode.get("name") == null ? XContentBoostFieldMapper.Defaults.NAME : boostNode.get("name").toString();
        XContentBoostFieldMapper.Builder builder = boost(name);
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

    private XContentTypeFieldMapper.Builder parseTypeField(Map<String, Object> typeNode, XContentTypeParser.ParserContext parserContext) {
        XContentTypeFieldMapper.Builder builder = type();
        parseField(builder, builder.name, typeNode, parserContext);
        return builder;
    }


    private XContentIdFieldMapper.Builder parseIdField(Map<String, Object> idNode, XContentTypeParser.ParserContext parserContext) {
        XContentIdFieldMapper.Builder builder = id();
        parseField(builder, builder.name, idNode, parserContext);
        return builder;
    }

    private XContentAllFieldMapper.Builder parseAllField(Map<String, Object> allNode, XContentTypeParser.ParserContext parserContext) {
        XContentAllFieldMapper.Builder builder = all();
        parseField(builder, builder.name, allNode, parserContext);
        for (Map.Entry<String, Object> entry : allNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }

    private XContentSourceFieldMapper.Builder parseSourceField(Map<String, Object> sourceNode, XContentTypeParser.ParserContext parserContext) {
        XContentSourceFieldMapper.Builder builder = source();

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
