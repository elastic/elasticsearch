/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.canMergeModelSettings;

/**
 * A mapper for the {@code _inference} field.
 * <br>
 * <br>
 * This mapper works in tandem with {@link SemanticTextFieldMapper semantic} fields to index inference results.
 * The inference results for {@code semantic} fields are written to {@code _source} by an upstream process like so:
 * <br>
 * <br>
 * <pre>
 * {
 *     "_source": {
 *         "my_semantic_text_field": "these are not the droids you're looking for",
 *         "_inference": {
 *             "my_semantic_text_field": {
 *                  "inference_id": "my_inference_id",
 *                  "model_settings": {
 *                      "task_type": "SPARSE_EMBEDDING"
 *                  },
 *                  "chunks" [
 *                      {
 *                          "inference": {
 *                              "lucas": 0.05212344,
 *                              "ty": 0.041213956,
 *                              "dragon": 0.50991,
 *                              "type": 0.23241979,
 *                              "dr": 1.9312073,
 *                              "##o": 0.2797593
 *                          },
 *                          "text": "these are not the droids you're looking for"
 *                      }
 *                  ]
 *              }
 *          }
 *      }
 * }
 * </pre>
 *
 * This mapper parses the contents of the {@code _inference} field and indexes it as if the mapping were configured like so:
 * <br>
 * <br>
 * <pre>
 * {
 *     "mappings": {
 *         "properties": {
 *             "my_semantic_field": {
 *                 "chunks": {
 *                      "type": "nested",
 *                      "properties": {
 *                          "embedding": {
 *                              "type": "sparse_vector|dense_vector"
 *                          },
 *                          "text": {
 *                              "type": "keyword",
 *                              "index": false,
 *                              "doc_values": false
 *                          }
 *                     }
 *                 }
 *             }
 *         }
 *     }
 * }
 * </pre>
 */
public class InferenceMetadataFieldMapper extends MetadataFieldMapper {
    public static final String NAME = InferenceFieldMapper.NAME;
    public static final String CONTENT_TYPE = "_inference";

    public static final String INFERENCE_ID = "inference_id";
    public static final String CHUNKS = "chunks";
    public static final String INFERENCE_CHUNKS_RESULTS = "inference";
    public static final String INFERENCE_CHUNKS_TEXT = "text";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new InferenceMetadataFieldMapper());

    private static final Logger logger = LogManager.getLogger(InferenceMetadataFieldMapper.class);

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

    public InferenceMetadataFieldMapper() {
        super(SemanticTextInferenceFieldType.INSTANCE);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        failIfTokenIsNot(parser.getTokenLocation(), parser, XContentParser.Token.START_OBJECT);
        boolean origWithLeafObject = context.path().isWithinLeafObject();
        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                failIfTokenIsNot(parser.getTokenLocation(), parser, XContentParser.Token.FIELD_NAME);
                parseSingleField(context);
            }
        } finally {
            context.path().setWithinLeafObject(origWithLeafObject);
        }
    }

    private NestedObjectMapper updateSemanticTextFieldMapper(
        DocumentParserContext docContext,
        SemanticTextMapperContext semanticFieldContext,
        String newInferenceId,
        SemanticTextModelSettings newModelSettings,
        XContentLocation xContentLocation
    ) {
        final String fullFieldName = semanticFieldContext.mapper.fieldType().name();
        final String inferenceId = semanticFieldContext.mapper.getInferenceId();
        if (newInferenceId.equals(inferenceId) == false) {
            throw new DocumentParsingException(
                xContentLocation,
                Strings.format(
                    "The configured %s [%s] for field [%s] doesn't match the %s [%s] reported in the document.",
                    INFERENCE_ID,
                    inferenceId,
                    fullFieldName,
                    INFERENCE_ID,
                    newInferenceId
                )
            );
        }
        if (newModelSettings.taskType() == TaskType.TEXT_EMBEDDING && newModelSettings.dimensions() == null) {
            throw new DocumentParsingException(
                xContentLocation,
                "Model settings for field [" + fullFieldName + "] must contain dimensions"
            );
        }
        if (semanticFieldContext.mapper.getModelSettings() == null) {
            SemanticTextFieldMapper newMapper = new SemanticTextFieldMapper.Builder(
                semanticFieldContext.mapper.simpleName(),
                docContext.indexSettings().getIndexVersionCreated()
            ).setInferenceId(newInferenceId).setModelSettings(newModelSettings).build(semanticFieldContext.context);
            docContext.addDynamicMapper(newMapper);
            return newMapper.getSubMappers();
        } else {
            SemanticTextFieldMapper.Conflicts conflicts = new Conflicts(fullFieldName);
            canMergeModelSettings(semanticFieldContext.mapper.getModelSettings(), newModelSettings, conflicts);
            try {
                conflicts.check();
            } catch (Exception exc) {
                throw new DocumentParsingException(xContentLocation, "Incompatible model_settings", exc);
            }
        }
        return semanticFieldContext.mapper.getSubMappers();
    }

    private void parseSingleField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        String fieldName = parser.currentName();
        SemanticTextMapperContext builderContext = createSemanticFieldContext(context, fieldName);
        if (builderContext == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                Strings.format("Field [%s] is not registered as a [%s] field type", fieldName, SemanticTextFieldMapper.CONTENT_TYPE)
            );
        }
        parser.nextToken();
        failIfTokenIsNot(parser.getTokenLocation(), parser, XContentParser.Token.START_OBJECT);

        // record the location of the inference field in the original source
        XContentLocation xContentLocation = parser.getTokenLocation();
        // parse eagerly to extract the inference id and the model settings first
        Map<String, Object> map = parser.mapOrdered();

        // inference_id
        Object inferenceIdObj = map.remove(INFERENCE_ID);
        final String inferenceId = XContentMapValues.nodeStringValue(inferenceIdObj, null);
        if (inferenceId == null) {
            throw new IllegalArgumentException("required [" + INFERENCE_ID + "] is missing");
        }

        // model_settings
        Object modelSettingsObj = map.remove(SemanticTextModelSettings.NAME);
        if (modelSettingsObj == null) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                Strings.format(
                    "Missing required [%s] for field [%s] of type [%s]",
                    SemanticTextModelSettings.NAME,
                    fieldName,
                    SemanticTextFieldMapper.CONTENT_TYPE
                )
            );
        }
        final SemanticTextModelSettings modelSettings;
        try {
            modelSettings = SemanticTextModelSettings.fromMap(modelSettingsObj);
        } catch (Exception exc) {
            throw new DocumentParsingException(
                xContentLocation,
                Strings.format(
                    "Error parsing [%s] for field [%s] of type [%s]",
                    SemanticTextModelSettings.NAME,
                    fieldName,
                    SemanticTextFieldMapper.CONTENT_TYPE
                ),
                exc
            );
        }

        var nestedObjectMapper = updateSemanticTextFieldMapper(context, builderContext, inferenceId, modelSettings, xContentLocation);

        // we know the model settings, so we can (re) parse the results array now
        XContentParser subParser = new MapXContentParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            map,
            XContentType.JSON
        );
        DocumentParserContext mapContext = context.switchParser(subParser);
        parseFieldInference(xContentLocation, subParser, mapContext, nestedObjectMapper);
    }

    private void parseFieldInference(
        XContentLocation xContentLocation,
        XContentParser parser,
        DocumentParserContext context,
        NestedObjectMapper nestedMapper
    ) throws IOException {
        parser.nextToken();
        failIfTokenIsNot(xContentLocation, parser, XContentParser.Token.START_OBJECT);
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (parser.currentName()) {
                case CHUNKS -> parseChunks(xContentLocation, parser, context, nestedMapper);
                default -> throw new DocumentParsingException(xContentLocation, "Unknown field name " + parser.currentName());
            }
        }
    }

    private void parseChunks(
        XContentLocation xContentLocation,
        XContentParser parser,
        DocumentParserContext context,
        NestedObjectMapper nestedMapper
    ) throws IOException {
        parser.nextToken();
        failIfTokenIsNot(xContentLocation, parser, XContentParser.Token.START_ARRAY);
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            DocumentParserContext subContext = context.createNestedContext(nestedMapper);
            parseResultsObject(xContentLocation, parser, subContext, nestedMapper);
        }
    }

    private void parseResultsObject(
        XContentLocation xContentLocation,
        XContentParser parser,
        DocumentParserContext context,
        NestedObjectMapper nestedMapper
    ) throws IOException {
        failIfTokenIsNot(xContentLocation, parser, XContentParser.Token.START_OBJECT);
        Set<String> visited = new HashSet<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            failIfTokenIsNot(xContentLocation, parser, XContentParser.Token.FIELD_NAME);
            visited.add(parser.currentName());
            FieldMapper fieldMapper = (FieldMapper) nestedMapper.getMapper(parser.currentName());
            if (fieldMapper == null) {
                if (REQUIRED_SUBFIELDS.contains(parser.currentName())) {
                    throw new DocumentParsingException(
                        xContentLocation,
                        "Missing sub-fields definition for [" + parser.currentName() + "]"
                    );
                } else {
                    logger.debug("Skipping indexing of unrecognized field name [" + parser.currentName() + "]");
                    advancePastCurrentFieldName(xContentLocation, parser);
                    continue;
                }
            }
            parser.nextToken();
            fieldMapper.parse(context);
        }
        if (visited.containsAll(REQUIRED_SUBFIELDS) == false) {
            Set<String> missingSubfields = REQUIRED_SUBFIELDS.stream()
                .filter(s -> visited.contains(s) == false)
                .collect(Collectors.toSet());
            throw new DocumentParsingException(xContentLocation, "Missing required subfields: " + missingSubfields);
        }
    }

    private static void failIfTokenIsNot(XContentLocation xContentLocation, XContentParser parser, XContentParser.Token expected) {
        if (parser.currentToken() != expected) {
            throw new DocumentParsingException(xContentLocation, "Expected a " + expected.toString() + ", got " + parser.currentToken());
        }
    }

    private static void advancePastCurrentFieldName(XContentLocation xContentLocation, XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.FIELD_NAME;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        } else if (token.isValue() == false && token != XContentParser.Token.VALUE_NULL) {
            throw new DocumentParsingException(xContentLocation, "Expected a START_* or VALUE_*, got " + token);
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

    public static void applyFieldInference(
        Map<String, Object> inferenceMap,
        String field,
        Model model,
        ChunkedInferenceServiceResults results
    ) throws ElasticsearchException {
        List<Map<String, Object>> chunks = new ArrayList<>();
        if (results instanceof ChunkedSparseEmbeddingResults textExpansionResults) {
            for (var chunk : textExpansionResults.getChunkedResults()) {
                chunks.add(chunk.asMap());
            }
        } else if (results instanceof ChunkedTextEmbeddingResults textEmbeddingResults) {
            for (var chunk : textEmbeddingResults.getChunks()) {
                chunks.add(chunk.asMap());
            }
        } else {
            throw new ElasticsearchStatusException(
                "Invalid inference results format for field [{}] with inference id [{}], got {}",
                RestStatus.BAD_REQUEST,
                field,
                model.getInferenceEntityId(),
                results.getWriteableName()
            );
        }
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        fieldMap.put(INFERENCE_ID, model.getInferenceEntityId());
        fieldMap.putAll(new SemanticTextModelSettings(model).asMap());
        fieldMap.put(CHUNKS, chunks);
        inferenceMap.put(field, fieldMap);
    }

    record SemanticTextMapperContext(MapperBuilderContext context, SemanticTextFieldMapper mapper) {}

    /**
     * Returns the {@link SemanticTextFieldMapper} associated with the provided {@code fullName}
     * and the {@link MapperBuilderContext} that was used to build it.
     * If the field is not found or is of the wrong type, this method returns {@code null}.
     */
    static SemanticTextMapperContext createSemanticFieldContext(DocumentParserContext docContext, String fullName) {
        ObjectMapper rootMapper = docContext.mappingLookup().getMapping().getRoot();
        return createSemanticFieldContext(MapperBuilderContext.root(false, false), rootMapper, fullName.split("\\."));
    }

    static SemanticTextMapperContext createSemanticFieldContext(
        MapperBuilderContext mapperContext,
        ObjectMapper objectMapper,
        String[] paths
    ) {
        Mapper mapper = objectMapper.getMapper(paths[0]);
        if (mapper instanceof ObjectMapper newObjectMapper) {
            mapperContext = mapperContext.createChildContext(paths[0], ObjectMapper.Dynamic.FALSE);
            return createSemanticFieldContext(mapperContext, newObjectMapper, Arrays.copyOfRange(paths, 1, paths.length));
        } else if (mapper instanceof SemanticTextFieldMapper semanticMapper) {
            return new SemanticTextMapperContext(mapperContext, semanticMapper);
        } else {
            if (mapper == null || paths.length == 1) {
                return null;
            }
            // check if the semantic field is defined within a multi-field
            Mapper fieldMapper = objectMapper.getMapper(String.join(".", Arrays.asList(paths)));
            if (fieldMapper instanceof SemanticTextFieldMapper semanticMapper) {
                return new SemanticTextMapperContext(mapperContext, semanticMapper);
            }
        }
        return null;
    }
}
