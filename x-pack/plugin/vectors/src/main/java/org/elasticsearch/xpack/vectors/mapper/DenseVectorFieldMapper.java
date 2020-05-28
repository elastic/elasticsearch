/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xpack.vectors.query.VectorIndexFieldData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 2048; //maximum allowed number of dimensions
    private static final byte INT_BYTES = 4;

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        private int dims = 0;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder dims(int dims) {
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException("The number of dimensions for field [" + name +
                    "] should be in the range [1, " + MAX_DIMS_COUNT + "] but was [" + dims + "]");
            }
            this.dims = dims;
            return this;
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            return new DenseVectorFieldMapper(
                    name, fieldType, new DenseVectorFieldType(buildFullName(context), dims, meta),
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(name);
            Object dimsField = node.remove("dims");
            if (dimsField == null) {
                throw new MapperParsingException("The [dims] property must be specified for field [" + name + "].");
            }
            int dims = XContentMapValues.nodeIntegerValue(dimsField);
            return builder.dims(dims);
        }
    }

    public static final class DenseVectorFieldType extends MappedFieldType {
        private final int dims;

        public DenseVectorFieldType(String name, int dims, Map<String, String> meta) {
            super(name, false, false, TextSearchInfo.NONE, meta);
            this.dims = dims;
        }

        int dims() {
            return dims;
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
            return new VectorIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support queries");
        }
    }

    private final Version indexCreatedVersion;

    private DenseVectorFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions() == IndexOptions.NONE;
        this.indexCreatedVersion = Version.indexCreated(indexSettings);
    }

    @Override
    protected DenseVectorFieldMapper clone() {
        return (DenseVectorFieldMapper) super.clone();
    }

    @Override
    public DenseVectorFieldType fieldType() {
        return (DenseVectorFieldType) super.fieldType();
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }
        int dims = fieldType().dims(); //number of vector dimensions

        // encode array of floats as array of integers and store into buf
        // this code is here and not int the VectorEncoderDecoder so not to create extra arrays
        byte[] bytes = indexCreatedVersion.onOrAfter(Version.V_7_5_0) ? new byte[dims * INT_BYTES + INT_BYTES] : new byte[dims * INT_BYTES];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        double dotProduct = 0f;

        int dim = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            if (dim++ >= dims) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                    context.sourceToParse().id() + "] has exceeded the number of dimensions [" + dims + "] defined in mapping");
            }
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser()::getTokenLocation);
            float value = context.parser().floatValue(true);

            byteBuffer.putFloat(value);
            dotProduct += value * value;
        }
        if (dim != dims) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" +
                context.sourceToParse().id() + "] has number of dimensions [" + dim +
                "] less than defined in the mapping [" +  dims +"]");
        }

        if (indexCreatedVersion.onOrAfter(Version.V_7_5_0)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        BinaryDocValuesField field = new BinaryDocValuesField(fieldType().name(), new BytesRef(bytes));
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
        }
        context.doc().addWithKey(fieldType().name(), field);
    }

    @Override
    protected Object parseSourceValue(Object value) {
        return value;
    }

    @Override
    protected boolean indexedByDefault() {
        return false;
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("dims", fieldType().dims());
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        DenseVectorFieldType otherType = (DenseVectorFieldType) other.fieldType();
        if (this.fieldType().dims() != otherType.dims()) {
            conflicts.add("mapper [" + name() + "] has different dims");
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
