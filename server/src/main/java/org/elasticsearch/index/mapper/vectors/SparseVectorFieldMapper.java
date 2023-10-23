/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField} as a sparse
 * vector of features.
 */
public class SparseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";

    static final String ERROR_MESSAGE_7X = "[sparse_vector] field type in old 7.x indices is allowed to "
        + "contain [sparse_vector] fields, but they cannot be indexed or searched.";
    static final String ERROR_MESSAGE_8X = "The [sparse_vector] field type is not supported from 8.0 to 8.10 versions.";
    static final IndexVersion PREVIOUS_SPARSE_VECTOR_INDEX_VERSION = IndexVersions.V_8_0_0;

    static final IndexVersion NEW_SPARSE_VECTOR_INDEX_VERSION = IndexVersions.NEW_SPARSE_VECTOR;
    static final IndexVersion SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION = IndexVersions.SPARSE_VECTOR_IN_FIELD_NAMES_SUPPORT;

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta };
        }

        @Override
        public SparseVectorFieldMapper build(MapperBuilderContext context) {
            return new SparseVectorFieldMapper(
                name,
                new SparseVectorFieldType(context.buildFullName(name), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        if (c.indexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            deprecationLogger.warn(DeprecationCategory.MAPPINGS, "sparse_vector", ERROR_MESSAGE_7X);
        } else if (c.indexVersionCreated().before(NEW_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new IllegalArgumentException(ERROR_MESSAGE_8X);
        }

        return new Builder(n);
    }, notInMultiFields(CONTENT_TYPE));

    public static final class SparseVectorFieldType extends MappedFieldType {

        public SparseVectorFieldType(String name, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[sparse_vector] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return FeatureField.newLinearQuery(name(), indexedValueForSearch(value), DEFAULT_BOOST);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (context.getIndexSettings().getIndexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
                deprecationLogger.warn(DeprecationCategory.MAPPINGS, "sparse_vector", ERROR_MESSAGE_7X);
                return new MatchNoDocsQuery();
            } else if (context.getIndexSettings().getIndexVersionCreated().before(SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION)) {
                // No support for exists queries prior to this version on 8.x
                throw new IllegalArgumentException("[sparse_vector] fields do not support [exists] queries");
            }
            return super.existsQuery(context);
        }

        private static String indexedValueForSearch(Object value) {
            if (value instanceof BytesRef) {
                return ((BytesRef) value).utf8ToString();
            }
            return value.toString();
        }
    }

    private SparseVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        // No support for indexing / searching 7.x sparse_vector field types
        if (context.indexSettings().getIndexVersionCreated().before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        } else if (context.indexSettings().getIndexVersionCreated().before(NEW_SPARSE_VECTOR_INDEX_VERSION)) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_8X);
        }

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[sparse_vector] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        String feature = null;
        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
                if (token == Token.FIELD_NAME) {
                    feature = context.parser().currentName();
                    if (feature.contains(".")) {
                        throw new IllegalArgumentException(
                            "[sparse_vector] fields do not support dots in feature names but found [" + feature + "]"
                        );
                    }
                } else if (token == Token.VALUE_NULL) {
                    // ignore feature, this is consistent with numeric fields
                } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                    final String key = name() + "." + feature;
                    float value = context.parser().floatValue(true);
                    if (context.doc().getByKey(key) != null) {
                        throw new IllegalArgumentException(
                            "[sparse_vector] fields do not support indexing multiple values for the same feature ["
                                + key
                                + "] in the same document"
                        );
                    }
                    context.doc().addWithKey(key, new FeatureField(name(), feature, value));
                } else {
                    throw new IllegalArgumentException(
                        "[sparse_vector] fields take hashes that map a feature to a strictly positive "
                            + "float, but got unexpected token "
                            + token
                    );
                }
            }
            if (context.indexSettings().getIndexVersionCreated().onOrAfter(SPARSE_VECTOR_IN_FIELD_NAMES_INDEX_VERSION)) {
                context.addToFieldNames(fieldType().name());
            }
        } finally {
            context.path().setWithinLeafObject(false);
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

}
