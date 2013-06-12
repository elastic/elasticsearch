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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.multifield.MultiFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.similarity.SimilarityLookupService;

import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.doc;

/**
 *
 */
public class DocumentMapperParser extends AbstractIndexComponent {

    final AnalysisService analysisService;
    private final PostingsFormatService postingsFormatService;
    private final DocValuesFormatService docValuesFormatService;
    private final SimilarityLookupService similarityLookupService;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Object typeParsersMutex = new Object();

    private volatile ImmutableMap<String, Mapper.TypeParser> typeParsers;
    private volatile ImmutableMap<String, Mapper.TypeParser> rootTypeParsers;

    public DocumentMapperParser(Index index, AnalysisService analysisService, PostingsFormatService postingsFormatService,
                                DocValuesFormatService docValuesFormatService, SimilarityLookupService similarityLookupService) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, analysisService, postingsFormatService, docValuesFormatService,
                similarityLookupService);
    }

    public DocumentMapperParser(Index index, @IndexSettings Settings indexSettings, AnalysisService analysisService,
                                PostingsFormatService postingsFormatService, DocValuesFormatService docValuesFormatService,
                                SimilarityLookupService similarityLookupService) {
        super(index, indexSettings);
        this.analysisService = analysisService;
        this.postingsFormatService = postingsFormatService;
        this.docValuesFormatService = docValuesFormatService;
        this.similarityLookupService = similarityLookupService;
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
                .put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser())
                .put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser())
                .put(MultiFieldMapper.CONTENT_TYPE, new MultiFieldMapper.TypeParser())
                .put(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser())
                .put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

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
                .put(AnalyzerMapper.NAME, new AnalyzerMapper.TypeParser())
                .put(BoostFieldMapper.NAME, new BoostFieldMapper.TypeParser())
                .put(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser())
                .put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser())
                .put(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser())
                .put(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser())
                .put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser())
                .put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser())
                .put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser())
                .immutableMap();
    }

    public void putTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            typeParsers = new MapBuilder<String, Mapper.TypeParser>(typeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    public void putRootTypeParser(String type, Mapper.TypeParser typeParser) {
        synchronized (typeParsersMutex) {
            rootTypeParsers = new MapBuilder<String, Mapper.TypeParser>()
                    .putAll(typeParsers)
                    .put(type, typeParser)
                    .immutableMap();
        }
    }

    public Mapper.TypeParser.ParserContext parserContext() {
        return new Mapper.TypeParser.ParserContext(postingsFormatService, docValuesFormatService, analysisService, similarityLookupService, typeParsers);
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

        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        if (defaultSource != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(MapperService.DEFAULT_MAPPING, defaultSource);
            if (t.v2() != null) {
                XContentHelper.mergeDefaults(mapping, t.v2());
            }
        }

        Mapper.TypeParser.ParserContext parserContext = new Mapper.TypeParser.ParserContext(postingsFormatService, docValuesFormatService, analysisService, similarityLookupService, typeParsers);

        DocumentMapper.Builder docBuilder = doc(index.name(), indexSettings, (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext));

        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();

            if ("index_analyzer".equals(fieldName)) {
                NamedAnalyzer analyzer = analysisService.analyzer(fieldNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + fieldNode.toString() + "] not found for index_analyzer setting on root type [" + type + "]");
                }
                docBuilder.indexAnalyzer(analyzer);
            } else if ("search_analyzer".equals(fieldName)) {
                NamedAnalyzer analyzer = analysisService.analyzer(fieldNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + fieldNode.toString() + "] not found for search_analyzer setting on root type [" + type + "]");
                }
                docBuilder.searchAnalyzer(analyzer);
            } else if ("search_quote_analyzer".equals(fieldName)) {
                NamedAnalyzer analyzer = analysisService.analyzer(fieldNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + fieldNode.toString() + "] not found for search_analyzer setting on root type [" + type + "]");
                }
                docBuilder.searchQuoteAnalyzer(analyzer);
            } else if ("analyzer".equals(fieldName)) {
                NamedAnalyzer analyzer = analysisService.analyzer(fieldNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + fieldNode.toString() + "] not found for analyzer setting on root type [" + type + "]");
                }
                docBuilder.indexAnalyzer(analyzer);
                docBuilder.searchAnalyzer(analyzer);
            } else {
                Mapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
                if (typeParser != null) {
                    docBuilder.put(typeParser.parse(fieldName, (Map<String, Object>) fieldNode, parserContext));
                }
            }
        }

        if (!docBuilder.hasIndexAnalyzer()) {
            docBuilder.indexAnalyzer(analysisService.defaultIndexAnalyzer());
        }
        if (!docBuilder.hasSearchAnalyzer()) {
            docBuilder.searchAnalyzer(analysisService.defaultSearchAnalyzer());
        }
        if (!docBuilder.hasSearchQuoteAnalyzer()) {
            docBuilder.searchAnalyzer(analysisService.defaultSearchQuoteAnalyzer());
        }

        ImmutableMap<String, Object> attributes = ImmutableMap.of();
        if (mapping.containsKey("_meta")) {
            attributes = ImmutableMap.copyOf((Map<String, Object>) mapping.get("_meta"));
        }
        docBuilder.meta(attributes);

        DocumentMapper documentMapper = docBuilder.build(this);
        // update the source with the generated one
        documentMapper.refreshSource();
        return documentMapper;
    }

    @SuppressWarnings({"unchecked"})
    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try {
            root = XContentFactory.xContent(source).createParser(source).mapOrderedAndClose();
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse mapping definition", e);
        }
        int size = root.size();
        switch (size) {
        case 0:
            // if we don't have any keys throw an exception
            throw new MapperParsingException("malformed mapping no root object found");
        case 1:
            break;
        default:
            // we always assume the first and single key is the mapping type root
            throw new MapperParsingException("mapping must have the `type` as the root object");

        }

        String rootName = root.keySet().iterator().next();
        if (type == null) {
            type = rootName;
        }

        return new Tuple<String, Map<String, Object>>(type, (Map<String, Object>) root.get(rootName));
    }
}
