/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.vectors.query.VectorDVIndexFieldData;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a sparse vector of floats.
 */
public class SparseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";
    public static short MAX_DIMS_COUNT = 1024; //maximum allowed number of dimensions
    public static int MAX_DIMS_NUMBER = 65535; //maximum allowed dimension's number

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

    public static class Builder extends FieldMapper.Builder<Builder, SparseVectorFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public SparseVectorFieldType fieldType() {
            return (SparseVectorFieldType) super.fieldType();
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
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            SparseVectorFieldMapper.Builder builder = new SparseVectorFieldMapper.Builder(name);
            return builder;
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new VectorDVIndexFieldData.Builder(false);
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
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        ensureExpectedToken(Token.START_OBJECT, context.parser().currentToken(), context.parser()::getTokenLocation);
        int[] dims = new int[0];
        float[] values = new float[0];
        int dimCount = 0;
        int dim = 0;
        float value;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                try {
                    dim = Integer.parseInt(context.parser().currentName());
                    if (dim < 0 || dim > MAX_DIMS_NUMBER) {
                        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "]'s dimension number " +
                            "must be a non-negative integer value not exceeding [" + MAX_DIMS_NUMBER + "], got [" + dim + "]");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "]'s dimensions should be " +
                        "integers represented as strings, but got [" + context.parser().currentName() + "]", e);
                }
            } else if (token == Token.VALUE_NUMBER) {
                value = context.parser().floatValue(true);
                if (dims.length <= dimCount) { // ensure arrays have enough capacity
                    values = ArrayUtil.grow(values, dimCount + 1);
                    dims = ArrayUtil.grow(dims, dimCount + 1);
                }
                dims[dimCount] = dim;
                values[dimCount] = value;
                if (dimCount++ >= MAX_DIMS_COUNT) {
                    throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                        "] has exceeded the maximum allowed number of dimensions of [" + MAX_DIMS_COUNT + "]");
                }
            } else {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                    "] takes an object that maps a dimension number to a float, " + "but got unexpected token [" + token + "]");
            }
        }

        BytesRef br = VectorEncoderDecoder.encodeSparseVector(indexCreatedVersion, dims, values, dimCount);
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), br);
        context.doc().addWithKey(fieldType().name(), field);
    }


    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
