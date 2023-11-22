/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SemanticTextInferenceResultFieldMapper extends MetadataFieldMapper {

    public static final String CONTENT_TYPE = "semantic_text_inference";

    public static final String NAME = "_semantic_text_inference";
    public static final String SPARSE_VECTOR_SUBFIELD_NAME = TaskType.SPARSE_EMBEDDING.toString();

    private static final SemanticTextInferenceResultFieldMapper INSTANCE = new SemanticTextInferenceResultFieldMapper();

    private static SemanticTextInferenceResultFieldMapper toType(FieldMapper in) {
        return (SemanticTextInferenceResultFieldMapper) in;
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> new SemanticTextInferenceResultFieldMapper());

    public static class SemanticTextInferenceFieldType extends MappedFieldType {

        public static final MappedFieldType INSTANCE = new SemanticTextInferenceFieldType();
        private SparseVectorFieldType sparseVectorFieldType;

        public SemanticTextInferenceFieldType() {
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
            return sparseVectorFieldType.termQuery(value, context);
        }
    }

    private SemanticTextInferenceResultFieldMapper() {
        super(SemanticTextInferenceFieldType.INSTANCE);
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        if (context.parser().currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[_semantic_text_inference] fields must be a json object, expected a START_OBJECT but got: "
                    + context.parser().currentToken()
            );
        }

        MapperBuilderContext mapperBuilderContext = MapperBuilderContext.root(false, false).createChildContext(NAME);

        // TODO Can we validate that semantic text fields have actual text values?
        for (XContentParser.Token token = context.parser().nextToken(); token != XContentParser.Token.END_OBJECT; token = context.parser()
            .nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("[semantic_text] fields expect an object with field names, found " + token);
            }

            String fieldName = context.parser().currentName();

            Mapper mapper = context.getMapper(fieldName);
            if (mapper == null) {
                // Not a field we have mapped? Must be model output, skip it
                context.parser().nextToken();
                context.path().setWithinLeafObject(true);
                Map<String, Object> fieldMap = context.parser().map();
                context.path().setWithinLeafObject(false);
                continue;
            }
            if (SemanticTextFieldMapper.CONTENT_TYPE.equals(mapper.typeName()) == false) {
                throw new IllegalArgumentException(
                    "Found [" + fieldName + "] in inference values, but it is not registered as a semantic_text field type"
                );
            }

            context.parser().nextToken();
            SparseVectorFieldMapper sparseVectorFieldMapper = new SparseVectorFieldMapper.Builder(fieldName).build(mapperBuilderContext);
            sparseVectorFieldMapper.parse(context);
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextInferenceFieldType fieldType() {
        return (SemanticTextInferenceFieldType) super.fieldType();
    }
}
