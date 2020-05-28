/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.time.ZoneId;
import java.util.List;
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
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(SparseVectorFieldMapper.class));
    static final String ERROR_MESSAGE = "The [sparse_vector] field type is no longer supported.";
    static final String ERROR_MESSAGE_7X = "The [sparse_vector] field type is no longer supported. Old 7.x indices are allowed to " +
        "contain [sparse_vector] fields, but they cannot be indexed or searched.";
    public static final String CONTENT_TYPE = "sparse_vector";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new SparseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public SparseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new SparseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            if (parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0)) {
                throw new IllegalArgumentException(ERROR_MESSAGE);
            } else {
                deprecationLogger.deprecatedAndMaybeLog("sparse_vector", ERROR_MESSAGE_7X);
                return new Builder(name);
            }
        }
    }

    public static final class SparseVectorFieldType extends MappedFieldType {

        public SparseVectorFieldType() {}

        protected SparseVectorFieldType(SparseVectorFieldType ref) {
            super(ref);
        }

        public SparseVectorFieldType clone() {
            return new SparseVectorFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support queries");
        }
    }


    private SparseVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                    Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions() == IndexOptions.NONE;
    }

    @Override
    protected SparseVectorFieldMapper clone() {
        return (SparseVectorFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) {
        throw new UnsupportedOperationException(ERROR_MESSAGE_7X);
    }


    @Override
    protected void parseCreateField(ParseContext context) {
        throw new IllegalStateException("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
