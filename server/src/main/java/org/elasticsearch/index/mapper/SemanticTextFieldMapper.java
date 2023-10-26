/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SparseVectorFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/** A {@link FieldMapper} for full-text fields. */
public class SemanticTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "semantic_text";
    private static final String SPARSE_VECTOR_SUFFIX = "_inference";

    private static final String TEXT_SUBFIELD_NAME = "text";
    private static final String SPARSE_VECTOR_SUBFIELD_NAME = "inference";

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<String> modelId = Parameter.stringParam("model_id", false, m -> toType(m).modelId, null).addValidator(value -> {
            if (value == null) {
                // TODO check the model exists
                throw new IllegalArgumentException("field [model_id] must be specified");
            }
        });

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        public Builder modelId(String modelId) {
            this.modelId.setValue(modelId);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                modelId,
                meta };
        }

        private SemanticTextFieldType buildFieldType(
            MapperBuilderContext context
        ) {
                return new SemanticTextFieldType(
                    context.buildFullName(name),
                    modelId.getValue(),
                    meta.getValue()
            );
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            SemanticTextFieldType stft = new SemanticTextFieldType(context.buildFullName(name), modelId.getValue(), meta.getValue());
            String fieldName = name() + SPARSE_VECTOR_SUFFIX;
            SubFieldInfo sparseVectorFieldInfo = new SubFieldInfo(fieldName, new SparseVectorFieldMapper.Builder(fieldName).build(context));
            return new SemanticTextFieldMapper(name, stft, modelId.getValue(), sparseVectorFieldInfo, copyTo, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) ->  new Builder(n), notInMultiFields(CONTENT_TYPE)
    );

    private static final class SubFieldInfo {

        private final SparseVectorFieldMapper sparseVectorFieldMapper;
        private final String fieldName;

        SubFieldInfo(String fieldName, SparseVectorFieldMapper sparseVectorFieldMapper) {
            this.fieldName = fieldName;
            this.sparseVectorFieldMapper = sparseVectorFieldMapper;
        }

    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType {

        private SparseVectorFieldType sparseVectorFieldType;

        private final String modelId;

        public SemanticTextFieldType(
            String name,
            String modelId,
            Map<String, String> meta
        ) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.modelId =  modelId;
        }

        public String modelId() {
            return modelId;
        }

        public SparseVectorFieldType getSparseVectorFieldType() {
            return this.sparseVectorFieldType;
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

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return sparseVectorFieldType.existsQuery(context);
        }
    }

    private final String modelId;
    private final SubFieldInfo sparseVectorFieldInfo;

    private SemanticTextFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        String modelId,
        SubFieldInfo sparseVectorFieldInfo,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
        this.modelId = modelId;
        this.sparseVectorFieldInfo = sparseVectorFieldInfo;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        context.parser();
        if (context.parser().currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[semantic_text] fields must be a json object, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        boolean textFound = false;
        boolean inferenceFound = false;
        for (XContentParser.Token token = context.parser().nextToken(); token != XContentParser.Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("[semantic_text] fields expect an object with field names, found " + token);
            }

            String fieldName = context.parser().currentName();
            XContentParser.Token valueToken = context.parser().nextToken();
            switch (fieldName) {
                case TEXT_SUBFIELD_NAME:
                    context.doc().add(new StringField(name() + TEXT_SUBFIELD_NAME, context.parser().textOrNull(), Field.Store.NO));
                    textFound = true;
                    break;
                case SPARSE_VECTOR_SUBFIELD_NAME:
                    sparseVectorFieldInfo.sparseVectorFieldMapper.parse(context);
                    inferenceFound = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected subfield value: " + fieldName);
            }
        }

        if (textFound == false) {
            throw new IllegalArgumentException("[semantic_text] value does not contain [" + TEXT_SUBFIELD_NAME + "] subfield");
        }
        if (inferenceFound == false) {
            throw new IllegalArgumentException("[semantic_text] value does not contain [" + SPARSE_VECTOR_SUBFIELD_NAME + "] subfield");
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }
}
