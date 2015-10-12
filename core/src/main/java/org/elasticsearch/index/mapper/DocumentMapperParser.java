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

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
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
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedMap;
import static org.elasticsearch.index.mapper.MapperBuilders.doc;

public class DocumentMapperParser {

    private final Settings indexSettings;
    final MapperService mapperService;
    final AnalysisService analysisService;
    private static final ESLogger logger = Loggers.getLogger(DocumentMapperParser.class);
    private final SimilarityService similarityService;
    private final ScriptService scriptService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();
    private final Version indexVersionCreated;
    private final ParseFieldMatcher parseFieldMatcher;

    private volatile Map<String, Mapper.TypeParser> typeParsers;
    private volatile Map<String, Mapper.TypeParser> rootTypeParsers;
    private volatile SortedMap<String, Mapper.TypeParser> additionalRootMappers;

    public DocumentMapperParser(@IndexSettings Settings indexSettings, MapperService mapperService, AnalysisService analysisService,
                                SimilarityService similarityService, ScriptService scriptService) {
        this.indexSettings = indexSettings;
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings);
        this.mapperService = mapperService;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.scriptService = scriptService;
        Map<String, Mapper.TypeParser> typeParsers = new HashMap<>();
        typeParsers.put(ByteFieldMapper.CONTENT_TYPE, new ByteFieldMapper.TypeParser());
        typeParsers.put(ShortFieldMapper.CONTENT_TYPE, new ShortFieldMapper.TypeParser());
        typeParsers.put(IntegerFieldMapper.CONTENT_TYPE, new IntegerFieldMapper.TypeParser());
        typeParsers.put(LongFieldMapper.CONTENT_TYPE, new LongFieldMapper.TypeParser());
        typeParsers.put(FloatFieldMapper.CONTENT_TYPE, new FloatFieldMapper.TypeParser());
        typeParsers.put(DoubleFieldMapper.CONTENT_TYPE, new DoubleFieldMapper.TypeParser());
        typeParsers.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        typeParsers.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        typeParsers.put(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        typeParsers.put(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        typeParsers.put(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
        typeParsers.put(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        typeParsers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        typeParsers.put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        typeParsers.put(TypeParsers.MULTI_FIELD_CONTENT_TYPE, TypeParsers.multiFieldConverterTypeParser);
        typeParsers.put(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
        typeParsers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE) {
            typeParsers.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }

        this.typeParsers = unmodifiableMap(typeParsers);

        Map<String, Mapper.TypeParser> rootTypeParsers = new HashMap<>();
        rootTypeParsers.put(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
        rootTypeParsers.put(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
        rootTypeParsers.put(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
        rootTypeParsers.put(AllFieldMapper.NAME, new AllFieldMapper.TypeParser());
        rootTypeParsers.put(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser());
        rootTypeParsers.put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
        rootTypeParsers.put(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser());
        rootTypeParsers.put(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser());
        rootTypeParsers.put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
        rootTypeParsers.put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
        rootTypeParsers.put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
        rootTypeParsers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
        this.rootTypeParsers = unmodifiableMap(rootTypeParsers);
        additionalRootMappers = Collections.emptySortedMap();
        indexVersionCreated = Version.indexCreated(indexSettings);
    }

    public void putTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            Map<String, Mapper.TypeParser> typeParsers = new HashMap<>(this.typeParsers);
            typeParsers.put(type, typeParser);
            this.typeParsers = unmodifiableMap(typeParsers);
        }
    }

    public void putRootTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            Map<String, Mapper.TypeParser> rootTypeParsers = new HashMap<>(this.rootTypeParsers);
            rootTypeParsers.put(type, typeParser);
            this.rootTypeParsers = rootTypeParsers;
            SortedMap<String, Mapper.TypeParser> additionalRootMappers = new TreeMap<>(this.additionalRootMappers);
            additionalRootMappers.put(type, typeParser);
            this.additionalRootMappers = unmodifiableSortedMap(additionalRootMappers);
        }
    }

    public Mapper.TypeParser.ParserContext parserContext(String type) {
        return new Mapper.TypeParser.ParserContext(type, analysisService, similarityService::getSimilarity, mapperService, typeParsers::get, indexVersionCreated, parseFieldMatcher);
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
            mapping = new HashMap<>();
        }
        return parse(type, mapping, defaultSource);
    }

    public DocumentMapper parseCompressed(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        return parseCompressed(type, source, null);
    }

    @SuppressWarnings({"unchecked"})
    public DocumentMapper parseCompressed(@Nullable String type, CompressedXContent source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Map<String, Object> root = XContentHelper.convertToMap(source.compressedReference(), true).v2();
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = new HashMap<>();
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


        Mapper.TypeParser.ParserContext parserContext = parserContext(type);
        // parse RootObjectMapper
        DocumentMapper.Builder docBuilder = doc(indexSettings, (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext), mapperService);
        // Add default mapping for the plugged-in meta mappers
        for (Map.Entry<String, Mapper.TypeParser> entry : additionalRootMappers.entrySet()) {
            docBuilder.put((MetadataFieldMapper.Builder<?, ?>) entry.getValue().parse(entry.getKey(), Collections.<String, Object>emptyMap(), parserContext));
        }
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if ("transform".equals(fieldName)) {
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
                iterator.remove();
            } else {
                Mapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
                if (typeParser != null) {
                    iterator.remove();
                    Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                    docBuilder.put((MetadataFieldMapper.Builder)typeParser.parse(fieldName, fieldNodeMap, parserContext));
                    fieldNodeMap.remove("type");
                    checkNoRemainingFields(fieldName, fieldNodeMap, parserContext.indexVersionCreated());
                }
            }
        }

        Map<String, Object> meta = (Map<String, Object>) mapping.remove("_meta");
        if (meta != null) {
            // It may not be required to copy meta here to maintain immutability
            // but the cost is pretty low here.
            docBuilder.meta(unmodifiableMap(new HashMap<>(meta)));
        }

        checkNoRemainingFields(mapping, parserContext.indexVersionCreated(), "Root mapping definition has unsupported parameters: ");

        return docBuilder.build(mapperService, this);
    }

    public static void checkNoRemainingFields(String fieldName, Map<String, Object> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(fieldNodeMap, indexVersionCreated, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<String, Object> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            if (indexVersionCreated.onOrAfter(Version.V_2_0_0_beta1)) {
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

    private void parseTransform(DocumentMapper.Builder docBuilder, Map<String, Object> transformConfig, Version indexVersionCreated) {
        Script script = Script.parse(transformConfig, true, parseFieldMatcher);
        if (script != null) {
            docBuilder.transform(scriptService, script);
        }
        checkNoRemainingFields(transformConfig, indexVersionCreated, "Transform config has unsupported parameters: ");
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            root = parser.mapOrdered();
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
