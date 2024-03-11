/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.SemanticTextModelSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_TEXT;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.ROOT_INFERENCE_FIELD;

/**
 * A mapper for the {@code _semantic_text_inference} field.
 * <br>
 * <br>
 * This mapper works in tandem with {@link SemanticTextFieldMapper semantic_text} fields to index inference results.
 * The inference results for {@code semantic_text} fields are written to {@code _source} by an upstream process like so:
 * <br>
 * <br>
 * <pre>
 * {
 *     "_source": {
 *         "my_semantic_text_field": "these are not the droids you're looking for",
 *         "_semantic_text_inference": {
 *             "my_semantic_text_field": [
 *                 {
 *                     "sparse_embedding": {
 *                          "lucas": 0.05212344,
 *                          "ty": 0.041213956,
 *                          "dragon": 0.50991,
 *                          "type": 0.23241979,
 *                          "dr": 1.9312073,
 *                          "##o": 0.2797593
 *                     },
 *                     "text": "these are not the droids you're looking for"
 *                 }
 *             ]
 *         }
 *     }
 * }
 * </pre>
 *
 * This mapper parses the contents of the {@code _semantic_text_inference} field and indexes it as if the mapping were configured like so:
 * <br>
 * <br>
 * <pre>
 * {
 *     "mappings": {
 *         "properties": {
 *             "my_semantic_text_field": {
 *                 "type": "nested",
 *                 "properties": {
 *                     "sparse_embedding": {
 *                         "type": "sparse_vector"
 *                     },
 *                     "text": {
 *                         "type": "text",
 *                         "index": false
 *                     }
 *                 }
 *             }
 *         }
 *     }
 * }
 * </pre>
 */
public class SemanticTextInferenceResultFieldMapper extends MetadataFieldMapper {
    public static final String CONTENT_TYPE = "_semantic_text_inference";
    public static final String NAME = ROOT_INFERENCE_FIELD;
    public static final TypeParser PARSER = new FixedTypeParser(c -> new SemanticTextInferenceResultFieldMapper());

    private static final Logger logger = LogManager.getLogger(SemanticTextInferenceResultFieldMapper.class);

    private static final Set<String> REQUIRED_SUBFIELDS = Set.of(INFERENCE_CHUNKS_TEXT, INFERENCE_CHUNKS_RESULTS);

    static class SemanticTextInferenceFieldType extends MappedFieldType {
        private static final MappedFieldType INSTANCE = new SemanticTextInferenceFieldType();

        SemanticTextInferenceFieldType() {
            super(NAME, true, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return null;
        }
    }

    private SemanticTextInferenceResultFieldMapper() {
        super(SemanticTextInferenceFieldType.INSTANCE);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        failIfTokenIsNot(parser, XContentParser.Token.START_OBJECT);

        parseAllFields(context);
    }

    private static void parseAllFields(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        MapperBuilderContext mapperBuilderContext = MapperBuilderContext.root(false, false);
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            failIfTokenIsNot(parser, XContentParser.Token.FIELD_NAME);

            parseSingleField(context, mapperBuilderContext);
        }
    }

    private static void parseSingleField(DocumentParserContext context, MapperBuilderContext mapperBuilderContext) throws IOException {

        XContentParser parser = context.parser();
        String fieldName = parser.currentName();
        Mapper mapper = context.getMapper(fieldName);
        if (mapper == null || SemanticTextFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                Strings.format("Field [%s] is not registered as a %s field type", fieldName, SemanticTextFieldMapper.CONTENT_TYPE)
            );
        }
        parser.nextToken();
        failIfTokenIsNot(parser, XContentParser.Token.START_OBJECT);
        parser.nextToken();
        SemanticTextModelSettings modelSettings = SemanticTextModelSettings.parse(parser);
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            failIfTokenIsNot(parser, XContentParser.Token.FIELD_NAME);

            String currentName = parser.currentName();
            if (BulkShardRequestInferenceProvider.INFERENCE_RESULTS.equals(currentName)) {
                NestedObjectMapper nestedObjectMapper = createInferenceResultsObjectMapper(
                    context,
                    mapperBuilderContext,
                    fieldName,
                    modelSettings
                );
                parseFieldInferenceChunks(context, mapperBuilderContext, fieldName, modelSettings, nestedObjectMapper);
            } else {
                logger.debug("Skipping unrecognized field name [" + currentName + "]");
                advancePastCurrentFieldName(parser);
            }
        }
    }

    private static void parseFieldInferenceChunks(
        DocumentParserContext context,
        MapperBuilderContext mapperBuilderContext,
        String fieldName,
        SemanticTextModelSettings modelSettings,
        NestedObjectMapper nestedObjectMapper
    ) throws IOException {
        XContentParser parser = context.parser();

        parser.nextToken();
        failIfTokenIsNot(parser, XContentParser.Token.START_ARRAY);

        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            DocumentParserContext nestedContext = context.createNestedContext(nestedObjectMapper);
            parseFieldInferenceChunkElement(nestedContext, nestedObjectMapper, modelSettings);
        }
    }

    private static void parseFieldInferenceChunkElement(
        DocumentParserContext context,
        ObjectMapper objectMapper,
        SemanticTextModelSettings modelSettings
    ) throws IOException {
        XContentParser parser = context.parser();
        DocumentParserContext childContext = context.createChildContext(objectMapper);

        failIfTokenIsNot(parser, XContentParser.Token.START_OBJECT);

        Set<String> visitedSubfields = new HashSet<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            failIfTokenIsNot(parser, XContentParser.Token.FIELD_NAME);

            String currentName = parser.currentName();
            visitedSubfields.add(currentName);

            Mapper childMapper = objectMapper.getMapper(currentName);
            if (childMapper == null) {
                logger.debug("Skipping indexing of unrecognized field name [" + currentName + "]");
                advancePastCurrentFieldName(parser);
                continue;
            }

            if (childMapper instanceof FieldMapper fieldMapper) {
                parser.nextToken();
                fieldMapper.parse(childContext);
            } else {
                // This should never happen, but fail parsing if it does so that it's not a silent failure
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    Strings.format("Unhandled mapper type [%s] for field [%s]", childMapper.getClass(), currentName)
                );
            }
        }

        if (visitedSubfields.containsAll(REQUIRED_SUBFIELDS) == false) {
            Set<String> missingSubfields = REQUIRED_SUBFIELDS.stream()
                .filter(s -> visitedSubfields.contains(s) == false)
                .collect(Collectors.toSet());
            throw new DocumentParsingException(parser.getTokenLocation(), "Missing required subfields: " + missingSubfields);
        }
    }

    private static NestedObjectMapper createInferenceResultsObjectMapper(
        DocumentParserContext context,
        MapperBuilderContext mapperBuilderContext,
        String fieldName,
        SemanticTextModelSettings modelSettings
    ) {
        IndexVersion indexVersionCreated = context.indexSettings().getIndexVersionCreated();
        FieldMapper.Builder resultsBuilder;
        if (modelSettings.taskType() == TaskType.SPARSE_EMBEDDING) {
            resultsBuilder = new SparseVectorFieldMapper.Builder(INFERENCE_CHUNKS_RESULTS);
        } else if (modelSettings.taskType() == TaskType.TEXT_EMBEDDING) {
            DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                INFERENCE_CHUNKS_RESULTS,
                indexVersionCreated
            );
            SimilarityMeasure similarity = modelSettings.similarity();
            if (similarity != null) {
                switch (similarity) {
                    case COSINE -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.COSINE);
                    case DOT_PRODUCT -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT);
                    default -> throw new IllegalArgumentException(
                        "Unknown similarity measure for field [" + fieldName + "] in model settings: " + similarity
                    );
                }
            }
            Integer dimensions = modelSettings.dimensions();
            if (dimensions == null) {
                throw new IllegalArgumentException("Model settings for field [" + fieldName + "] must contain dimensions");
            }
            denseVectorMapperBuilder.dimensions(dimensions);
            resultsBuilder = denseVectorMapperBuilder;
        } else {
            throw new IllegalArgumentException("Unknown task type for field [" + fieldName + "]: " + modelSettings.taskType());
        }

        TextFieldMapper.Builder textMapperBuilder = new TextFieldMapper.Builder(
            INFERENCE_CHUNKS_TEXT,
            indexVersionCreated,
            context.indexAnalyzers()
        ).index(false).store(false);

        NestedObjectMapper.Builder nestedBuilder = new NestedObjectMapper.Builder(
            fieldName,
            context.indexSettings().getIndexVersionCreated()
        );
        nestedBuilder.add(resultsBuilder).add(textMapperBuilder);

        return nestedBuilder.build(mapperBuilderContext);
    }

    private static void advancePastCurrentFieldName(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.FIELD_NAME;

        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        } else if (token.isValue() == false && token != XContentParser.Token.VALUE_NULL) {
            throw new DocumentParsingException(parser.getTokenLocation(), "Expected a START_* or VALUE_*, got " + token);
        }
    }

    private static void failIfTokenIsNot(XContentParser parser, XContentParser.Token expected) {
        if (parser.currentToken() != expected) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "Expected a " + expected.toString() + ", got " + parser.currentToken()
            );
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
