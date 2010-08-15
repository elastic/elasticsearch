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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentMerger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentDocumentMapperParser extends AbstractIndexComponent implements DocumentMapperParser {

    private final AnalysisService analysisService;

    private final XContentObjectMapper.TypeParser rootObjectTypeParser = new XContentObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();

    private volatile ImmutableMap<String, XContentTypeParser> typeParsers;

    public XContentDocumentMapperParser(Index index, AnalysisService analysisService) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, analysisService);
    }

    public XContentDocumentMapperParser(Index index, @IndexSettings Settings indexSettings, AnalysisService analysisService) {
        super(index, indexSettings);
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
                .put(XContentGeoPointFieldMapper.CONTENT_TYPE, new XContentGeoPointFieldMapper.TypeParser())
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

    @Override public XContentDocumentMapper parse(String source) throws MapperParsingException {
        return parse(null, source);
    }

    @Override public XContentDocumentMapper parse(@Nullable String type, String source) throws MapperParsingException {
        return parse(type, source, null);
    }

    @Override public XContentDocumentMapper parse(@Nullable String type, String source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(type, source);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = Maps.newHashMap();
        }

        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        if (defaultSource != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(MapperService.DEFAULT_MAPPING, defaultSource);
            if (t.v2() != null) {
                XContentMerger.mergeDefaults(mapping, t.v2());
            }
        }

        XContentTypeParser.ParserContext parserContext = new XContentTypeParser.ParserContext(mapping, analysisService, typeParsers);

        XContentDocumentMapper.Builder docBuilder = doc(index.name(), (XContentObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext));

        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if (XContentSourceFieldMapper.CONTENT_TYPE.equals(fieldName) || "sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentIdFieldMapper.CONTENT_TYPE.equals(fieldName) || "idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((Map<String, Object>) fieldNode, parserContext));
            } else if (XContentIndexFieldMapper.CONTENT_TYPE.equals(fieldName) || "indexField".equals(fieldName)) {
                docBuilder.indexField(parseIndexField((Map<String, Object>) fieldNode, parserContext));
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

        ImmutableMap<String, Object> attributes = ImmutableMap.of();
        if (mapping.containsKey("_attributes")) {
            attributes = ImmutableMap.copyOf((Map<String, Object>) mapping.get("_attributes"));
        }
        docBuilder.attributes(attributes);

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
            } else if (fieldName.equals("compress") && fieldNode != null) {
                builder.compress(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }

    private XContentIndexFieldMapper.Builder parseIndexField(Map<String, Object> indexNode, XContentTypeParser.ParserContext parserContext) {
        XContentIndexFieldMapper.Builder builder = XContentMapperBuilders.index();
        parseField(builder, builder.name, indexNode, parserContext);

        for (Map.Entry<String, Object> entry : indexNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            }
        }
        return builder;
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
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

        // we always assume the first and single key is the mapping type root
        if (root.keySet().size() != 1) {
            throw new MapperParsingException("Mapping must have the `type` as the root object");
        }

        String rootName = root.keySet().iterator().next();
        if (type == null) {
            type = rootName;
        }

        return new Tuple<String, Map<String, Object>>(type, (Map<String, Object>) root.get(rootName));
    }
}
