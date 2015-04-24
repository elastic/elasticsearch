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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.Murmur3FieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TokenCountFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SizeFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.doc;

/**
 *
 */
public class DocumentMapperParser extends AbstractIndexComponent {

    final AnalysisService analysisService;
    private static final ESLogger logger = Loggers.getLogger(DocumentMapperParser.class);
    private final SimilarityLookupService similarityLookupService;
    private final ScriptService scriptService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();
    private final Version indexVersionCreated;

    private volatile ImmutableMap<String, Mapper.TypeParser> typeParsers;
    private volatile ImmutableMap<String, Mapper.TypeParser> rootTypeParsers;

    public DocumentMapperParser(Index index, @IndexSettings Settings indexSettings, AnalysisService analysisService,
                                SimilarityLookupService similarityLookupService, ScriptService scriptService) {
        super(index, indexSettings);
        this.analysisService = analysisService;
        this.similarityLookupService = similarityLookupService;
        this.scriptService = scriptService;
        MapBuilder<String, Mapper.TypeParser> typeParsersBuilder = new MapBuilder<String, Mapper.TypeParser>()
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
                .put(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser())
                .put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser())
                .put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser())
                .put(TypeParsers.MULTI_FIELD_CONTENT_TYPE, TypeParsers.multiFieldConverterTypeParser)
                .put(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser())
                .put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser())
                .put(Murmur3FieldMapper.CONTENT_TYPE, new Murmur3FieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE) {
            typeParsersBuilder.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }

        typeParsers = typeParsersBuilder.immutableMap();

        rootTypeParsers = new MapBuilder<String, Mapper.TypeParser>()
                .put(SizeFieldMapper.NAME, new SizeFieldMapper.TypeParser())
                .put(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser())
                .put(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser())
                .put(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser())
                .put(AllFieldMapper.NAME, new AllFieldMapper.TypeParser())
                .put(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser())
                .put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser())
                .put(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser())
                .put(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser())
                .put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser())
                .put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser())
                .put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser())
                .put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser())
                .immutableMap();
        indexVersionCreated = Version.indexCreated(indexSettings);
    }

    public void putTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            typeParsers = new MapBuilder<>(typeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    public void putRootTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            rootTypeParsers = new MapBuilder<>(rootTypeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    public Mapper.TypeParser.ParserContext parserContext() {
        return new Mapper.TypeParser.ParserContext(analysisService, similarityLookupService, typeParsers, indexVersionCreated);
    }

    public DocumentMapper parse(String source) throws MapperParsingException {
        return parse(null, source);
    }

    public DocumentMapper parse(@Nullable String type, String source) throws MapperParsingException {
        return parse(type, source, null);
    }

    @SuppressWarnings({"unchecked"})
    public DocumentMapper parse(@Nullable String type, String source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(type, source);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = Maps.newHashMap();
        }
        return parse(type, mapping, defaultSource);
    }

    public DocumentMapper parseCompressed(@Nullable String type, CompressedString source) throws MapperParsingException {
        return parseCompressed(type, source, null);
    }

    @SuppressWarnings({"unchecked"})
    public DocumentMapper parseCompressed(@Nullable String type, CompressedString source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Map<String, Object> root = XContentHelper.convertToMap(source.compressed(), true).v2();
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = Maps.newHashMap();
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


        Mapper.TypeParser.ParserContext parserContext = parserContext();
        // parse RootObjectMapper
        DocumentMapper.Builder docBuilder = doc(index.name(), indexSettings, (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext));
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if ("transform".equals(fieldName)) {
                iterator.remove();
                if (fieldNode instanceof Map) {
                    parseTransform(docBuilder, (Map<String, Object>) fieldNode, parserContext.indexVersionCreated());
                } else if (fieldNode instanceof List) {
                    for (Object transformItem: (List)fieldNode) {
                        if (!(transformItem instanceof Map)) {
                            throw new MapperParsingException("Elements of transform list must be objects but one was:  " + fieldNode);
                        }
                        parseTransform(docBuilder, (Map<String, Object>) transformItem, parserContext.indexVersionCreated());
                    }
                } else {
                    throw new MapperParsingException("Transform must be an object or an array but was:  " + fieldNode);
                }
            } else {
                Mapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
                if (typeParser != null) {
                    iterator.remove();
                    Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                    docBuilder.put(typeParser.parse(fieldName, fieldNodeMap, parserContext));
                    fieldNodeMap.remove("type");
                    checkNoRemainingFields(fieldName, fieldNodeMap, parserContext.indexVersionCreated());
                }
            }
        }

        ImmutableMap<String, Object> attributes = ImmutableMap.of();
        if (mapping.containsKey("_meta")) {
            attributes = ImmutableMap.copyOf((Map<String, Object>) mapping.remove("_meta"));
        }
        docBuilder.meta(attributes);

        checkNoRemainingFields(mapping, parserContext.indexVersionCreated(), "Root mapping definition has unsupported parameters: ");

        DocumentMapper documentMapper = docBuilder.build(this);
        // update the source with the generated one
        documentMapper.refreshSource();
        return documentMapper;
    }

    public static void checkNoRemainingFields(String fieldName, Map<String, Object> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(fieldNodeMap, indexVersionCreated, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<String, Object> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            if (indexVersionCreated.onOrAfter(Version.V_2_0_0)) {
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

    @SuppressWarnings("unchecked")
    private void parseTransform(DocumentMapper.Builder docBuilder, Map<String, Object> transformConfig, Version indexVersionCreated) {
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        scriptParameterParser.parseConfig(transformConfig, true);
        
        String script = null;
        ScriptType scriptType = null;
        ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
        if (scriptValue != null) {
            script = scriptValue.script();
            scriptType = scriptValue.scriptType();
        }
        
        if (script != null) {
            String scriptLang = scriptParameterParser.lang();
            Map<String, Object> params = (Map<String, Object>)transformConfig.remove("params");
            docBuilder.transform(scriptService, script, scriptType, scriptLang, params);
        }
        checkNoRemainingFields(transformConfig, indexVersionCreated, "Transform config has unsupported parameters: ");
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try {
            root = XContentFactory.xContent(source).createParser(source).mapOrderedAndClose();
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
