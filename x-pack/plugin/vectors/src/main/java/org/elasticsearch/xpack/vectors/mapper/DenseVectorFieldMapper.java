/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArrayValueMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.vectors.query.VectorDVIndexFieldData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 2048; //maximum allowed number of dimensions

    public static final byte INT_BYTES = 4;
    public static final int MAX_PCENTROIDS_COUNT = 256; // number of product centroids in each product quantizer
    public static final String KMEANS_ALGORITHM_LLOYDS = "lloyds";
    public static final String KMEANS_ALGORITHM_SORT = "sort";
    private static final int DEFAULT_PQ_COUNT = 8;
    private static final int DEFAULT_KMEANS_ITER = 5;
    private static final float DEFAULT_KMEANS_SAMPLE_FRACTION = 1.0f;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DenseVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DenseVectorFieldMapper> {
        private int dims = 0;
        private String ann = null;
        private int pqCount = DEFAULT_PQ_COUNT;
        private int kmeansIters = DEFAULT_KMEANS_ITER;
        private float kmeansSampleFraction = DEFAULT_KMEANS_SAMPLE_FRACTION;
        private String kmeansAlgorithm = KMEANS_ALGORITHM_LLOYDS;
        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder dims(int dims) {
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException("The number of dimensions for field [" + name +
                    "] should be in the range [1, " + MAX_DIMS_COUNT + "]");
            }
            this.dims = dims;
            return this;
        }

        public Builder ann(String ann) {
            if (ann.equals("pq") == false) {
                throw new MapperParsingException("[ann] value for field [" + name + "] should be [pq]!");
            }
            this.ann = ann;
            return this;
        }

        // TODO: check that dims is divided by pqCount without remainder
        public Builder pqCount(int pqCount) {
            this.pqCount = pqCount;
            return this;
        }

        public Builder kmeansIters(int kmeansIters) {
            this.kmeansIters = kmeansIters;
            return this;
        }

        public Builder kmeansSampleFraction(float kmeansSampleFraction) {
            this.kmeansSampleFraction = kmeansSampleFraction;
            return this;
        }

        public Builder kmeansAlgorithm(String kmeansAlgorithm) {
            if (kmeansAlgorithm.equals(KMEANS_ALGORITHM_LLOYDS) || kmeansAlgorithm.equals(KMEANS_ALGORITHM_SORT)) {
                this.kmeansAlgorithm = kmeansAlgorithm;
            } else {
                throw new MapperParsingException("[kmeans_algorithm] value for field [" + name + "] should be [lloyds] or [sort]!");
            }
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setDims(dims);
            if (ann != null) {
                fieldType().setAnn(ann);
                fieldType().setPqCount(pqCount);
                fieldType().setKmeansIters(kmeansIters);
                fieldType().setkmeansSampleFraction(kmeansSampleFraction);
                fieldType().setKmeansAlgorithm(kmeansAlgorithm);
                fieldType().putAttribute("ann", ann);
                fieldType().putAttribute("dims", String.valueOf(dims));
                fieldType().putAttribute("pqCount", String.valueOf(pqCount));
                fieldType().putAttribute("kmeansIters", String.valueOf(kmeansIters));
                fieldType().putAttribute("kmeansSampleFraction", String.valueOf(kmeansSampleFraction));
                fieldType().putAttribute("kmeansAlgorithm", kmeansAlgorithm);
                fieldType().setDocValuesType(DocValuesType.BINARY);
            }
        }

        @Override
        public DenseVectorFieldType fieldType() {
            return (DenseVectorFieldType) super.fieldType();
        }

        @Override
        public DenseVectorFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DenseVectorFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DenseVectorFieldMapper.Builder builder = new DenseVectorFieldMapper.Builder(name);
            Object dimsField = node.remove("dims");
            if (dimsField == null) {
                throw new MapperParsingException("The [dims] property must be specified for field [" + name + "].");
            }
            int dims = XContentMapValues.nodeIntegerValue(dimsField);
            builder.dims(dims);

            Object annField = node.remove("ann");
            if (annField != null) {
                String ann = XContentMapValues.nodeStringValue(annField, "pq");
                builder.ann(ann);

                Object pqCountField = node.remove("product_quantizers_count");
                if (pqCountField != null) {
                    int pqCount = XContentMapValues.nodeIntegerValue(pqCountField);
                    builder.pqCount(pqCount);
                }

                Object kmeansItersField = node.remove("kmeans_iters");
                if (kmeansItersField != null) {
                    int kmeansIters = XContentMapValues.nodeIntegerValue(kmeansItersField);
                    builder.kmeansIters(kmeansIters);
                }

                Object kmeansSampleFractionField = node.remove("kmeans_sample_fraction");
                if (kmeansSampleFractionField != null) {
                    float kmeansSampleFraction = XContentMapValues.nodeFloatValue(kmeansSampleFractionField);
                    builder.kmeansSampleFraction(kmeansSampleFraction);
                }

                Object kmeandAlgorithmField = node.remove("kmeans_algorithm"); //lloyds or sort
                if (kmeandAlgorithmField != null) {
                    String algorithm = XContentMapValues.nodeStringValue(kmeandAlgorithmField, "lloyds");
                    builder.kmeansAlgorithm(algorithm);
                }
            }
            return builder;
        }
    }

    public static final class DenseVectorFieldType extends MappedFieldType {
        private int dims;
        private String ann = null; // an <ann> indexing method
        private int pqCount; // number of product quantizers
        private int kmeansIters;
        private float kmeansSampleFraction;
        private String kmeansAlgorithm;

        public DenseVectorFieldType() {}

        protected DenseVectorFieldType(DenseVectorFieldType ref) {
            super(ref);
        }

        public DenseVectorFieldType clone() {
            return new DenseVectorFieldType(this);
        }

        public int dims() {
            return dims;
        }

        public String ann() {
            return ann;
        }

        int kmeansIters() {
            return kmeansIters;
        }

        String kmeansAlgorithm() {
            return kmeansAlgorithm;
        }

        public int pqCount() {
            return pqCount;
        }

        void setDims(int dims) {
            this.dims = dims;
        }

        void setAnn(String ann) {
            this.ann = ann;
        }

        void setKmeansIters(int kmeansIters) {
            this.kmeansIters = kmeansIters;
        }

        void setkmeansSampleFraction(float kmeansSampleFraction) {
            this.kmeansSampleFraction = kmeansSampleFraction;
        }

        void setKmeansAlgorithm(String kmeansAlgorithm) {
            this.kmeansAlgorithm = kmeansAlgorithm;
        }

        void setPqCount(int pqCount) {
            this.pqCount = pqCount;
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
            return new VectorDVIndexFieldData.Builder();
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException(
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support queries");
        }
    }

    private DenseVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                   Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions() == IndexOptions.NONE;
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
        Field field;
        if (fieldType().ann() == null) {
            field = new BinaryDocValuesField(fieldType().name(), new BytesRef(bytes));
        } else {
            field = new Field(fieldType().name(), new BytesRef(bytes), fieldType());
        }
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                "] doesn't not support indexing multiple values for the same field in the same document");
        }
        context.doc().addWithKey(fieldType().name(), field);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("dims", fieldType().dims());
        if (fieldType().ann != null) {
            builder.field("ann", fieldType().ann);
            builder.field("product_quantizers_count", fieldType().pqCount);
            builder.field("kmeans_iters", fieldType().kmeansIters);
            builder.field("kmeans_sample_fraction", fieldType().kmeansSampleFraction);
            builder.field("kmeans_algorithm", fieldType().kmeansAlgorithm);
        }
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
