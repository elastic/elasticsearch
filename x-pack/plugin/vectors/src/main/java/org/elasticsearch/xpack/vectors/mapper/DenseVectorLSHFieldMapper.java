/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.vectors.models.LSHModelEuclideanMultiProbe;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for LSH indexing of a dense vector of floats
 */
public class DenseVectorLSHFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "dense_vector_lsh";
    public static short MAX_DIMS_COUNT = 1024; // maximum allowed number of dimensions

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DenseVectorLSHFieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, DenseVectorLSHFieldMapper> {
        private int dims = 0;
        private short l = 0; // number of hash tables
        private int k = 0; // number of hash functions within each hash table
        private int w = 0; // width of the projection //TODO: should this be float?
        private float[][][] a = null; // parameters for LSH model
        private float[][] b; // parameters for LSH model
        private int[][] pertSets; // parameters for LSH model
        private LSHModelEuclideanMultiProbe lshModel; // LSH model

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public DenseVectorLSHFieldType fieldType() {
            return (DenseVectorLSHFieldType) super.fieldType();
        }

        public Builder dims(int dims) {
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException("The number of dimensions for the field [" + name() +
                    "] should be in the range [1, " + MAX_DIMS_COUNT + "]");
            }
            this.dims = dims;
            return this;
        }
        public int dims() {
            return dims;
        }
        public Builder l(short l) {
            // TODO: upper limit for l?
            if (l < 1) {
                throw new MapperParsingException(
                    "The number of hash tables [l] for the field [" + name() + "] should be greater than 0]");
            }
            this.l = l;
            return this;
        }
        public short l() {
            return l;
        }
        public Builder k(int k) {
            // TODO: upper limit for k?
            if (k < 1) {
                throw new MapperParsingException(
                    "The number of hash functions [k] for the field [" + name() + "] should be greater than 0]");
            }
            this.k = k;
            return this;
        }
        public int k() {
            return k;
        }
        public Builder w(int w) {
            // TODO: lower/upper limit for w?
            if (w < 1) {
                throw new MapperParsingException(
                    "The  width of the projection [w] for the field [" + name() + "] should be greater than 0]");
            }
            this.w = w;
            return this;
        }
        public int w() {
            return w;
        }
        public Builder a(float[][][] a) {
            this.a = a;
            return this;
        }
        public Builder b(float[][] b) {
            this.b = b;
            return this;
        }
        public Builder pertSets(int[][] pertSets) {
            this.pertSets = pertSets;
            return this;
        }
        public Builder buildLshModel() {
            if (a == null) { // the 1st time field mapping created, model hasn't built yet - build it
                lshModel = new LSHModelEuclideanMultiProbe(l, k, w, dims);
            } else { // model has already been built before — initialize it
                lshModel = new LSHModelEuclideanMultiProbe(l, k, w, dims, a, b, pertSets);
            }
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType().setDims(dims);
            fieldType().setLSHModel(lshModel);
        }

        @Override
        public DenseVectorLSHFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DenseVectorLSHFieldMapper(
                name, fieldType, defaultFieldType, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("dims")) {
                    builder.dims(XContentMapValues.nodeIntegerValue(propNode));
                    iterator.remove();
                } else if (propName.equals("l")) {
                    builder.l(XContentMapValues.nodeShortValue(propNode));
                    iterator.remove();
                } else if (propName.equals("k")) {
                    builder.k(XContentMapValues.nodeIntegerValue(propNode));
                    iterator.remove();
                } else if (propName.equals("w")) {
                    builder.w(XContentMapValues.nodeIntegerValue(propNode));
                    iterator.remove();
                // a, b and pertSets are not user-defined, they are read from the stored mapping
                } else if (propName.equals("a")) {
                    builder.a(node3DFloatArrayValue(propNode));
                    iterator.remove();
                } else if (propName.equals("b")) {
                    builder.b(node2DFloatArrayValue(propNode));
                    iterator.remove();
                } else if (propName.equals("pertSets")) {
                    builder.pertSets(node2DIntArrayValue(propNode));
                    iterator.remove();
                }
            }
            if (builder.dims() == 0) { // dims was not set
                throw new MapperParsingException("The [dims] property must be specified for the field [" + name + "].");
            }
            if (builder.l() == 0) { // l was not set
                throw new MapperParsingException("The [l] property must be specified for the field [" + name + "].");
            }
            if (builder.k() == 0) { // k was not set
                throw new MapperParsingException("The [k] property must be specified for the field [" + name + "].");
            }
            if (builder.w() == 0) { // w was not set
                throw new MapperParsingException("The [w] property must be specified for the field [" + name + "].");
            }
            builder.buildLshModel();
            return builder;
        }
    }

    public static final class DenseVectorLSHFieldType extends MappedFieldType {
        private int dims;
        private LSHModelEuclideanMultiProbe lshModel;

        public DenseVectorLSHFieldType() {}

        protected DenseVectorLSHFieldType(DenseVectorLSHFieldType ref) {
            super(ref);
        }

        public DenseVectorLSHFieldType clone() {
            return new DenseVectorLSHFieldType(this);
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
            return null;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return new TermQuery(new Term(name(), (BytesRef) value));
        }

        int dims() {
            return dims;
        }

        void setDims(int dims) {
            this.dims = dims;
        }

        public LSHModelEuclideanMultiProbe lshModel() {
            return lshModel;
        }

        void setLSHModel(LSHModelEuclideanMultiProbe lshModel) {
            this.lshModel = lshModel;
        }
    }

    protected DenseVectorLSHFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                        Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected DenseVectorLSHFieldMapper clone() {
        return (DenseVectorLSHFieldMapper) super.clone();
    }

    @Override
    public DenseVectorLSHFieldType fieldType() {
        return (DenseVectorLSHFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // TODO: implement usage in multi-fields
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] can't be used in multi-fields");
        }

        int dims = fieldType().lshModel.dims(); // number of vector dimensions
        float[] vector = new float[dims];
        int dim = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            if (dim >= dims) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" + context.doc() +
                    "] has number of dimensions [" + dim + "] greater than defined in the mapping: [" +  dims +"]");
            }
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser()::getTokenLocation);
            vector[dim] = context.parser().floatValue(true);
            dim++;
        }
        if (dim != dims) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] of doc [" + context.doc() +
                "] has number of dimensions [" + dim + "] less than defined in the mapping: [" +  dims +"]");
        }

        // hash vector to get L number of hashes corresponding to L hash tables
        BytesRef[] hashes = fieldType().lshModel().hash(vector);
        // index each hash as an untokenized term
        String fieldNameLSH = fieldType().name();
        for (int i = 0; i < hashes.length; i++) {
            Field indexedField = new Field(fieldNameLSH, hashes[i], fieldType());
            context.doc().add(indexedField);
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

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("dims", fieldType().dims());
        builder.field("l", fieldType().lshModel.l());
        builder.field("k", fieldType().lshModel.k());
        builder.field("w", fieldType().lshModel.w());
        builder.field("a", fieldType().lshModel.a());
        builder.field("b", fieldType().lshModel.b());
        builder.field("pertSets", fieldType().lshModel.pertSets());
    }

    // helper methods
    @SuppressWarnings({"unchecked"})
    private static int[] nodeIntArrayValue(Object node) {
        List<Number> list = (List) node;
        int[] arr = new int[list.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = XContentMapValues.nodeIntegerValue(list.get(i));
        }
        return arr;
    }

    @SuppressWarnings({"unchecked"})
    private static int[][] node2DIntArrayValue(Object node) {
        List<List<Number>> list = (List) node;
        int[][] arr = new int[list.size()][];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = nodeIntArrayValue(list.get(i));
        }
        return arr;
    }

    @SuppressWarnings({"unchecked"})
    private static float[] nodeFloatArrayValue(Object node) {
        List<Number> list = (List) node;
        float[] arr = new float[list.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = XContentMapValues.nodeFloatValue(list.get(i));
        }
        return arr;
    }

    @SuppressWarnings({"unchecked"})
    private static float[][] node2DFloatArrayValue(Object node) {
        List<List<Number>> list = (List) node;
        float[][] arr = new float[list.size()][];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = nodeFloatArrayValue(list.get(i));
        }
        return arr;
    }

    @SuppressWarnings({"unchecked"})
    private static float[][][] node3DFloatArrayValue(Object node) {
        List<List<List<Number>>> list = (List) node;
        float[][][] arr = new float[list.size()][][];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = node2DFloatArrayValue(list.get(i));
        }
        return arr;
    }
}
