/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceModelFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

/**
 *  A {@link FieldMapper} for semantic text fields. These fields have a model id reference, that is used for performing inference
 * at ingestion and query time.
 * For now, it is compatible with text expansion models only, but will be extended to support dense vector models as well.
 * This field mapper performs no indexing, as inference results will be included as a different field in the document source, and will
 * be indexed using {@link InferenceResultFieldMapper}.
 */
public class SemanticTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "semantic_text";

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n), notInMultiFields(CONTENT_TYPE));

    private SemanticTextFieldMapper(String simpleName, MappedFieldType mappedFieldType, CopyTo copyTo) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        // Just parses text - no indexing is performed
        context.parser().textOrNull();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<String> modelId = Parameter.stringParam("model_id", false, m -> toType(m).fieldType().modelId, null)
            .addValidator(v -> {
                if (Strings.isEmpty(v)) {
                    throw new IllegalArgumentException("field [model_id] must be specified");
                }
            });

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { modelId, meta };
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            return new SemanticTextFieldMapper(name(), new SemanticTextFieldType(name(), modelId.getValue(), meta.getValue()), copyTo);
        }
    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType implements InferenceModelFieldType {

        private final String modelId;

        public SemanticTextFieldType(String name, String modelId, Map<String, String> meta) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.modelId = modelId;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String getInferenceModel() {
            return modelId;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("termQuery not implemented yet");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }
    }
}
