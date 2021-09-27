/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.vectors.query.VectorIndexFieldData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 2048; //maximum allowed number of dimensions
    private static final byte INT_BYTES = 4;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Integer> dims
            = new Parameter<>("dims", false, () -> null, (n, c, o) -> XContentMapValues.nodeIntegerValue(o), m -> toType(m).dims)
            .addValidator(dims -> {
                if (dims == null) {
                    throw new MapperParsingException("Missing required parameter [dims] for field [" + name + "]");
                }
                if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                    throw new MapperParsingException("The number of dimensions for field [" + name +
                        "] should be in the range [1, " + MAX_DIMS_COUNT + "] but was [" + dims + "]");
                }
            });

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, false);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final Version indexVersionCreated;

        public Builder(String name, Version indexVersionCreated) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(dims, indexed, meta);
        }

        @Override
        public DenseVectorFieldMapper build(MapperBuilderContext context) {
            return new DenseVectorFieldMapper(
                name,
                new DenseVectorFieldType(context.buildFullName(name), indexVersionCreated,
                    dims.getValue(), indexed.getValue(), meta.getValue()),
                dims.getValue(),
                indexed.getValue(),
                indexVersionCreated,
                multiFieldsBuilder.build(this, context),
                copyTo.build());
        }
    }

    public static final TypeParser PARSER
        = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated()), notInMultiFields(CONTENT_TYPE));

    public static final class DenseVectorFieldType extends SimpleMappedFieldType {
        private final int dims;
        private final boolean indexed;
        private final Version indexVersionCreated;

        public DenseVectorFieldType(String name, Version indexVersionCreated, int dims, boolean indexed, Map<String, String> meta) {
            super(name, indexed, false, false, TextSearchInfo.NONE, meta);
            this.dims = dims;
            this.indexed = indexed;
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            throw new IllegalArgumentException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations");
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            return new VectorIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, indexVersionCreated, dims, indexed);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support queries");
        }
    }

    private final int dims;
    private final boolean indexed;
    private final Version indexCreatedVersion;

    private DenseVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, int dims, boolean indexed,
                                   Version indexCreatedVersion, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.dims = dims;
        this.indexed = indexed;
        this.indexCreatedVersion = indexCreatedVersion;
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
    public void parse(DocumentParserContext context) throws IOException {
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
        }

        Field field = fieldType().indexed
            ? parseKnnVector(context)
            : parseBinaryDocValuesVector(context);
        context.doc().addWithKey(fieldType().name(), field);
    }

    private Field parseKnnVector(DocumentParserContext context) throws IOException {
        float[] vector = new float[dims];
        int index = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            checkDimensionExceeded(index, context);
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
            vector[index++] = context.parser().floatValue(true);
        }
        checkDimensionMatches(index, context);
        return new KnnVectorField(fieldType().name(), vector);
    }

    private Field parseBinaryDocValuesVector(DocumentParserContext context) throws IOException {
        // encode array of floats as array of integers and store into buf
        // this code is here and not int the VectorEncoderDecoder so not to create extra arrays
        byte[] bytes = indexCreatedVersion.onOrAfter(Version.V_7_5_0) ? new byte[dims * INT_BYTES + INT_BYTES] : new byte[dims * INT_BYTES];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        double dotProduct = 0f;

        int index = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            checkDimensionExceeded(index, context);
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
            float value = context.parser().floatValue(true);
            byteBuffer.putFloat(value);
            dotProduct += value * value;
            index++;
        }
        checkDimensionMatches(index, context);

        if (indexCreatedVersion.onOrAfter(Version.V_7_5_0)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        return new BinaryDocValuesField(fieldType().name(), new BytesRef(bytes));
    }

    private void checkDimensionExceeded(int index, DocumentParserContext context) {
        if (index >= dims) {
            throw new IllegalArgumentException("The [" + typeName() + "] field [" + name() +
                "] in doc [" + context.sourceToParse().id() + "] has more dimensions " +
                "than defined in the mapping [" + dims + "]");
        }
    }

    private void checkDimensionMatches(int index, DocumentParserContext context) {
        if (index != dims) {
            throw new IllegalArgumentException("The [" + typeName() + "] field [" + name() +
                "] in doc [" + context.sourceToParse().id() + "] has a different number of dimensions " +
                "[" + index + "] than defined in the mapping [" + dims + "]");
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
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexCreatedVersion).init(this);
    }
}
