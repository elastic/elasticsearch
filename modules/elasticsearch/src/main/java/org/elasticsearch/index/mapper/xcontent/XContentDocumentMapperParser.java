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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.xcontent.ip.IpFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentDocumentMapperParser extends AbstractIndexComponent implements DocumentMapperParser {

    final AnalysisService analysisService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();

    private volatile ImmutableMap<String, XContentMapper.TypeParser> typeParsers;

    public XContentDocumentMapperParser(Index index, AnalysisService analysisService) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, analysisService);
    }

    public XContentDocumentMapperParser(Index index, @IndexSettings Settings indexSettings, AnalysisService analysisService) {
        super(index, indexSettings);
        this.analysisService = analysisService;
        typeParsers = new MapBuilder<String, XContentMapper.TypeParser>()
                .put(ByteFieldMapper.CONTENT_TYPE, new ByteFieldMapper.TypeParser())
                .put(ShortFieldMapper.CONTENT_TYPE, new ShortFieldMapper.TypeParser())
                .put(IntegerFieldMapper.CONTENT_TYPE, new IntegerFieldMapper.TypeParser())
                .put(LongFieldMapper.CONTENT_TYPE, new LongFieldMapper.TypeParser())
                .put(FloatFieldMapper.CONTENT_TYPE, new FloatFieldMapper.TypeParser())
                .put(DoubleFieldMapper.CONTENT_TYPE, new DoubleFieldMapper.TypeParser())
                .put(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser())
                .put(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser())
                .put(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser())
                .put(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser())
                .put(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser())
                .put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser())
                .put(MultiFieldMapper.CONTENT_TYPE, new MultiFieldMapper.TypeParser())
                .put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser())
                .immutableMap();
    }

    public void putTypeParser(String type, XContentMapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            typeParsers = new MapBuilder<String, XContentMapper.TypeParser>()
                    .putAll(typeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    public XContentMapper.TypeParser.ParserContext parserContext() {
        return new XContentMapper.TypeParser.ParserContext(analysisService, typeParsers);
    }

    @Override public XContentDocumentMapper parse(String source) throws MapperParsingException {
        return parse(null, source);
    }

    @Override public XContentDocumentMapper parse(@Nullable String type, String source) throws MapperParsingException {
        return parse(type, source, null);
    }

    @SuppressWarnings({"unchecked"})
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
                XContentHelper.mergeDefaults(mapping, t.v2());
            }
        }

        XContentMapper.TypeParser.ParserContext parserContext = new XContentMapper.TypeParser.ParserContext(analysisService, typeParsers);

        XContentDocumentMapper.Builder docBuilder = doc(index.name(), indexSettings, (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext));

        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if (SourceFieldMapper.CONTENT_TYPE.equals(fieldName) || "sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((Map<String, Object>) fieldNode, parserContext));
            } else if (SizeFieldMapper.CONTENT_TYPE.equals(fieldName)) {
                docBuilder.sizeField(parseSizeField((Map<String, Object>) fieldNode, parserContext));
            } else if (IdFieldMapper.CONTENT_TYPE.equals(fieldName) || "idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((Map<String, Object>) fieldNode, parserContext));
            } else if (IndexFieldMapper.CONTENT_TYPE.equals(fieldName) || "indexField".equals(fieldName)) {
                docBuilder.indexField(parseIndexField((Map<String, Object>) fieldNode, parserContext));
            } else if (TypeFieldMapper.CONTENT_TYPE.equals(fieldName) || "typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((Map<String, Object>) fieldNode, parserContext));
            } else if (UidFieldMapper.CONTENT_TYPE.equals(fieldName) || "uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((Map<String, Object>) fieldNode, parserContext));
            } else if (RoutingFieldMapper.CONTENT_TYPE.equals(fieldName)) {
                docBuilder.routingField(parseRoutingField((Map<String, Object>) fieldNode, parserContext));
            } else if (ParentFieldMapper.CONTENT_TYPE.equals(fieldName)) {
                docBuilder.parentFiled(parseParentField((Map<String, Object>) fieldNode, parserContext));
            } else if (BoostFieldMapper.CONTENT_TYPE.equals(fieldName) || "boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((Map<String, Object>) fieldNode, parserContext));
            } else if (AllFieldMapper.CONTENT_TYPE.equals(fieldName) || "allField".equals(fieldName)) {
                docBuilder.allField(parseAllField((Map<String, Object>) fieldNode, parserContext));
            } else if (AnalyzerMapper.CONTENT_TYPE.equals(fieldName)) {
                docBuilder.analyzerField(parseAnalyzerField((Map<String, Object>) fieldNode, parserContext));
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
        if (mapping.containsKey("_meta")) {
            attributes = ImmutableMap.copyOf((Map<String, Object>) mapping.get("_meta"));
        }
        docBuilder.meta(attributes);

        XContentDocumentMapper documentMapper = docBuilder.build(this);
        // update the source with the generated one
        documentMapper.refreshSource();
        return documentMapper;
    }

    private UidFieldMapper.Builder parseUidField(Map<String, Object> uidNode, XContentMapper.TypeParser.ParserContext parserContext) {
        UidFieldMapper.Builder builder = uid();
        return builder;
    }

    private BoostFieldMapper.Builder parseBoostField(Map<String, Object> boostNode, XContentMapper.TypeParser.ParserContext parserContext) {
        String name = boostNode.get("name") == null ? BoostFieldMapper.Defaults.NAME : boostNode.get("name").toString();
        BoostFieldMapper.Builder builder = boost(name);
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

    private TypeFieldMapper.Builder parseTypeField(Map<String, Object> typeNode, XContentMapper.TypeParser.ParserContext parserContext) {
        TypeFieldMapper.Builder builder = type();
        parseField(builder, builder.name, typeNode, parserContext);
        return builder;
    }


    private IdFieldMapper.Builder parseIdField(Map<String, Object> idNode, XContentMapper.TypeParser.ParserContext parserContext) {
        IdFieldMapper.Builder builder = id();
        parseField(builder, builder.name, idNode, parserContext);
        return builder;
    }

    // NOTE, we also parse this in MappingMetaData
    private RoutingFieldMapper.Builder parseRoutingField(Map<String, Object> routingNode, XContentMapper.TypeParser.ParserContext parserContext) {
        RoutingFieldMapper.Builder builder = routing();
        parseField(builder, builder.name, routingNode, parserContext);
        for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("required")) {
                builder.required(nodeBooleanValue(fieldNode));
            } else if (fieldName.equals("path")) {
                builder.path(fieldNode.toString());
            }
        }
        return builder;
    }

    private ParentFieldMapper.Builder parseParentField(Map<String, Object> parentNode, XContentMapper.TypeParser.ParserContext parserContext) {
        ParentFieldMapper.Builder builder = new ParentFieldMapper.Builder();
        for (Map.Entry<String, Object> entry : parentNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("type")) {
                builder.type(fieldNode.toString());
            }
        }
        return builder;
    }

    private AnalyzerMapper.Builder parseAnalyzerField(Map<String, Object> analyzerNode, XContentMapper.TypeParser.ParserContext parserContext) {
        AnalyzerMapper.Builder builder = analyzer();
        for (Map.Entry<String, Object> entry : analyzerNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("path")) {
                builder.field(fieldNode.toString());
            }
        }
        return builder;
    }

    private AllFieldMapper.Builder parseAllField(Map<String, Object> allNode, XContentMapper.TypeParser.ParserContext parserContext) {
        AllFieldMapper.Builder builder = all();
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

    private SizeFieldMapper.Builder parseSizeField(Map<String, Object> node, XContentMapper.TypeParser.ParserContext parserContext) {
        SizeFieldMapper.Builder builder = new SizeFieldMapper.Builder();

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            } else if (fieldName.equals("store")) {
                builder.store(parseStore(fieldName, fieldNode.toString()));
            }
        }
        return builder;
    }

    private SourceFieldMapper.Builder parseSourceField(Map<String, Object> sourceNode, XContentMapper.TypeParser.ParserContext parserContext) {
        SourceFieldMapper.Builder builder = source();

        for (Map.Entry<String, Object> entry : sourceNode.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
            } else if (fieldName.equals("compress") && fieldNode != null) {
                builder.compress(nodeBooleanValue(fieldNode));
            } else if (fieldName.equals("compress_threshold") && fieldNode != null) {
                if (fieldNode instanceof Number) {
                    builder.compressThreshold(((Number) fieldNode).longValue());
                    builder.compress(true);
                } else {
                    builder.compressThreshold(ByteSizeValue.parseBytesSizeValue(fieldNode.toString()).bytes());
                    builder.compress(true);
                }
            }
        }
        return builder;
    }

    private IndexFieldMapper.Builder parseIndexField(Map<String, Object> indexNode, XContentMapper.TypeParser.ParserContext parserContext) {
        IndexFieldMapper.Builder builder = XContentMapperBuilders.index();
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

    @SuppressWarnings({"unchecked"})
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
