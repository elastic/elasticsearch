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
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
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
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.*;

import static org.elasticsearch.index.mapper.MapperBuilders.doc;

public class DocumentMapperParser {

    private final Settings indexSettings;
    final MapperService mapperService;
    final AnalysisService analysisService;
    private static final ESLogger logger = Loggers.getLogger(DocumentMapperParser.class);
    private final SimilarityLookupService similarityLookupService;
    private final ScriptService scriptService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();
    private final Version indexVersionCreated;
    private final ParseFieldMatcher parseFieldMatcher;

    private volatile ImmutableMap<String, Mapper.TypeParser> typeParsers;
    private volatile ImmutableMap<String, Mapper.TypeParser> rootTypeParsers;
    private volatile SortedMap<String, Mapper.TypeParser> additionalRootMappers;

    public DocumentMapperParser(@IndexSettings Settings indexSettings, MapperService mapperService, AnalysisService analysisService,
                                SimilarityLookupService similarityLookupService, ScriptService scriptService) {
        this.indexSettings = indexSettings;
        this.parseFieldMatcher = new ParseFieldMatcher(indexSettings);
        this.mapperService = mapperService;
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
                .put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE) {
            typeParsersBuilder.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }

        typeParsers = typeParsersBuilder.immutableMap();

        rootTypeParsers = new MapBuilder<String, Mapper.TypeParser>()
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
        additionalRootMappers = Collections.emptySortedMap();
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
            SortedMap<String, Mapper.TypeParser> newAdditionalRootMappers = new TreeMap<>();
            newAdditionalRootMappers.putAll(additionalRootMappers);
            newAdditionalRootMappers.put(type, typeParser);
            additionalRootMappers = Collections.unmodifiableSortedMap(newAdditionalRootMappers);
        }
    }

    public Mapper.TypeParser.ParserContext parserContext(String type) {
        return new Mapper.TypeParser.ParserContext(type, analysisService, similarityLookupService, mapperService, typeParsers, indexVersionCreated, parseFieldMatcher);
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

        ImmutableMap<String, Object> attributes = ImmutableMap.of();
        if (mapping.containsKey("_meta")) {
            attributes = ImmutableMap.copyOf((Map<String, Object>) mapping.remove("_meta"));
        }
        docBuilder.meta(attributes);

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
