/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
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
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: fill in blanks in javadoc
/**
 * A mapper for the {@code _semantic_text_inference} field.
 * <br>
 * <br>
 * This mapper works in tandem with {@link SemanticTextFieldMapper semantic_text} fields to index inference results.
 * The inference results for {@code semantic_text} fields are written to {@code _source} by ______ like so:
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
 *                         "is_truncated": false,
 *                         "embedding": {
 *                             "lucas": 0.05212344,
 *                             "ty": 0.041213956,
 *                             "dragon": 0.50991,
 *                             "type": 0.23241979,
 *                             "dr": 1.9312073,
 *                             "##o": 0.2797593
 *                         }
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
 *                         "embedding": {
 *                             "type": "sparse_vector"
 *                         }
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
    public static final String NAME = "_semantic_text_inference";
    public static final String SPARSE_VECTOR_SUBFIELD_NAME = "sparse_embedding";
    public static final String TEXT_SUBFIELD_NAME = "text";
    public static final TypeParser PARSER = new FixedTypeParser(c -> new SemanticTextInferenceResultFieldMapper());

    private static final Set<String> REQUIRED_SUBFIELDS = Set.of(SPARSE_VECTOR_SUBFIELD_NAME, TEXT_SUBFIELD_NAME);

    // TODO: Need to query this as a nested field type?
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
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new DocumentParsingException(parser.getTokenLocation(), "Expected a START_OBJECT, got " + parser.currentToken());
        }

        parseInferenceResults(context);
    }

    private static void parseInferenceResults(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        MapperBuilderContext mapperBuilderContext = MapperBuilderContext.root(false, false);
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new DocumentParsingException(parser.getTokenLocation(), "Expected a FIELD_NAME, got " + token);
            }

            parseFieldInferenceResults(context, mapperBuilderContext);
        }
    }

    private static void parseFieldInferenceResults(DocumentParserContext context, MapperBuilderContext mapperBuilderContext)
        throws IOException {

        String fieldName = context.parser().currentName();
        Mapper mapper = context.getMapper(fieldName);
        if (mapper == null || SemanticTextFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                Strings.format("Field [%s] is not registered as a %s field type", fieldName, SemanticTextFieldMapper.CONTENT_TYPE)
            );
        }

        parseFieldInferenceResultsArray(context, mapperBuilderContext, fieldName);
    }

    private static void parseFieldInferenceResultsArray(
        DocumentParserContext context,
        MapperBuilderContext mapperBuilderContext,
        String fieldName
    ) throws IOException {

        XContentParser parser = context.parser();
        NestedObjectMapper nestedObjectMapper = createNestedObjectMapper(context, mapperBuilderContext, fieldName);
        context.path().add(fieldName); // TODO: Need path manipulation?

        try {
            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw new DocumentParsingException(parser.getTokenLocation(), "Expected a START_ARRAY, got " + parser.currentToken());
            }

            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                DocumentParserContext nestedContext = context.createNestedContext(nestedObjectMapper);
                parseObject(nestedContext, nestedObjectMapper, REQUIRED_SUBFIELDS);
            }
        } finally {
            context.path().remove();
        }
    }

    private static void parseObject(DocumentParserContext context, ObjectMapper objectMapper, Set<String> requiredSubfields)
        throws IOException {
        XContentParser parser = context.parser();
        DocumentParserContext childContext = context.createChildContext(objectMapper);

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new DocumentParsingException(parser.getTokenLocation(), "Expected a START_OBJECT, got " + parser.currentToken());
        }

        Set<String> visitedSubfields = new HashSet<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new DocumentParsingException(parser.getTokenLocation(), "Expected a FIELD_NAME, got " + parser.currentToken());
            }

            String currentName = parser.currentName();
            visitedSubfields.add(currentName);

            // TODO: Test missing/extra field handling
            Mapper childMapper = objectMapper.getMapper(currentName);
            if (childMapper == null) {
                throw new DocumentParsingException(parser.getTokenLocation(), "Unexpected field name: " + currentName);
            }

            if (childMapper instanceof FieldMapper) {
                parser.nextToken();
                ((FieldMapper) childMapper).parse(childContext);
            } else if (childMapper instanceof ObjectMapper) {
                parser.nextToken();
                parseObject(childContext, (ObjectMapper) childMapper, null);
            } else {
                // This should never happen, but fail parsing if it does so that it's not a silent failure
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    Strings.format("Unhandled mapper type [%s] for field [%s]", childMapper.getClass(), currentName)
                );
            }
        }

        if (requiredSubfields != null && visitedSubfields.containsAll(requiredSubfields) == false) {
            Set<String> missingSubfields = requiredSubfields.stream()
                .filter(s -> visitedSubfields.contains(s) == false)
                .collect(Collectors.toSet());
            throw new DocumentParsingException(parser.getTokenLocation(), "Missing required subfields: " + missingSubfields);
        }
    }

    private static NestedObjectMapper createNestedObjectMapper(
        DocumentParserContext context,
        MapperBuilderContext mapperBuilderContext,
        String fieldName
    ) {

        // TODO: Use keyword field type for text?
        // TODO: Why add text field to mapper if it's not indexed or stored?
        IndexVersion indexVersionCreated = context.indexSettings().getIndexVersionCreated();
        ObjectMapper.Builder sparseVectorMapperBuilder = new ObjectMapper.Builder(
            SPARSE_VECTOR_SUBFIELD_NAME,
            ObjectMapper.Defaults.SUBOBJECTS
        ).add(
            new BooleanFieldMapper.Builder(SparseEmbeddingResults.Embedding.IS_TRUNCATED, ScriptCompiler.NONE, false, indexVersionCreated)
        ).add(new SparseVectorFieldMapper.Builder(SparseEmbeddingResults.Embedding.EMBEDDING));
        TextFieldMapper.Builder textMapperBuilder = new TextFieldMapper.Builder(
            TEXT_SUBFIELD_NAME,
            indexVersionCreated,
            context.indexAnalyzers()
        ).index(false).store(false);

        NestedObjectMapper.Builder nestedBuilder = new NestedObjectMapper.Builder(
            fieldName,
            context.indexSettings().getIndexVersionCreated()
        );
        nestedBuilder.add(sparseVectorMapperBuilder).add(textMapperBuilder);

        return nestedBuilder.build(mapperBuilderContext);
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
