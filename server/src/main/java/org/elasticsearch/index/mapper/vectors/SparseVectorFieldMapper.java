/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;
import java.util.Map;

/**
 * A {@link FieldMapper} for indexing a sparse vector of floats.
 *
 * @deprecated The sparse_vector type was deprecated in 7.x and removed in 8.0. This mapper
 * definition only exists so that 7.x indices can be read without error.
 *
 * TODO: remove in 9.0.
 */
@Deprecated
public class SparseVectorFieldMapper extends FieldMapper {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SparseVectorFieldMapper.class);
    static final String ERROR_MESSAGE = "The [sparse_vector] field type is no longer supported.";
    static final String ERROR_MESSAGE_7X = "The [sparse_vector] field type is no longer supported. Old 7.x indices are allowed to "
        + "contain [sparse_vector] fields, but they cannot be indexed or searched.";
    public static final String CONTENT_TYPE = "sparse_vector";

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

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
                copyTo.build()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> {
        if (c.indexVersionCreated().onOrAfter(IndexVersion.V_8_0_0)) {
            throw new IllegalArgumentException(ERROR_MESSAGE);
        } else {
            deprecationLogger.warn(DeprecationCategory.MAPPINGS, "sparse_vector", ERROR_MESSAGE_7X);
            return new Builder(n);
        }
    });

    public static final class SparseVectorFieldType extends MappedFieldType {

        public SparseVectorFieldType(String name, Map<String, String> meta) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
        }
    }

    private SparseVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(DocumentParserContext context) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new IllegalStateException("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
