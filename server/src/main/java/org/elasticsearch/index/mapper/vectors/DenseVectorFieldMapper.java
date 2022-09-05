/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94HnswVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParser;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 2048; // maximum allowed number of dimensions
    private static final byte INT_BYTES = 4;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<Integer> dims = new Parameter<>(
            "dims",
            false,
            () -> null,
            (n, c, o) -> XContentMapValues.nodeIntegerValue(o),
            m -> toType(m).dims,
            XContentBuilder::field,
            Objects::toString
        ).addValidator(dims -> {
            if (dims == null) {
                throw new MapperParsingException("Missing required parameter [dims] for field [" + name + "]");
            }
            if ((dims > MAX_DIMS_COUNT) || (dims < 1)) {
                throw new MapperParsingException(
                    "The number of dimensions for field ["
                        + name
                        + "] should be in the range [1, "
                        + MAX_DIMS_COUNT
                        + "] but was ["
                        + dims
                        + "]"
                );
            }
        });

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, false);
        private final Parameter<VectorSimilarity> similarity = Parameter.enumParam(
            "similarity",
            false,
            m -> toType(m).similarity,
            null,
            VectorSimilarity.class
        );
        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            "index_options",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : parseIndexOptions(n, o),
            m -> toType(m).indexOptions,
            XContentBuilder::field,
            Objects::toString
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final Version indexVersionCreated;

        public Builder(String name, Version indexVersionCreated) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;

            this.indexed.requiresParameter(similarity);
            this.similarity.setSerializerCheck((id, ic, v) -> v != null);
            this.similarity.requiresParameter(indexed);
            this.indexOptions.requiresParameter(indexed);
            this.indexOptions.setSerializerCheck((id, ic, v) -> v != null);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { dims, indexed, similarity, indexOptions, meta };
        }

        @Override
        public DenseVectorFieldMapper build(MapperBuilderContext context) {
            return new DenseVectorFieldMapper(
                name,
                new DenseVectorFieldType(
                    context.buildFullName(name),
                    indexVersionCreated,
                    dims.getValue(),
                    indexed.getValue(),
                    similarity.getValue(),
                    meta.getValue()
                ),
                dims.getValue(),
                indexed.getValue(),
                similarity.getValue(),
                indexOptions.getValue(),
                indexVersionCreated,
                multiFieldsBuilder.build(this, context),
                copyTo.build()
            );
        }
    }

    enum VectorSimilarity {
        l2_norm(VectorSimilarityFunction.EUCLIDEAN),
        cosine(VectorSimilarityFunction.COSINE),
        dot_product(VectorSimilarityFunction.DOT_PRODUCT);

        public final VectorSimilarityFunction function;

        VectorSimilarity(VectorSimilarityFunction function) {
            this.function = function;
        }
    }

    private abstract static class IndexOptions implements ToXContent {
        final String type;

        IndexOptions(String type) {
            this.type = type;
        }
    }

    private static class HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;

        static IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
            Object mNode = indexOptionsMap.remove("m");
            Object efConstructionNode = indexOptionsMap.remove("ef_construction");
            if (mNode == null) {
                throw new MapperParsingException("[index_options] of type [hnsw] requires field [m] to be configured");
            }
            if (efConstructionNode == null) {
                throw new MapperParsingException("[index_options] of type [hnsw] requires field [ef_construction] to be configured");
            }
            int m = XContentMapValues.nodeIntegerValue(mNode);
            int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode);
            MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
            return new HnswIndexOptions(m, efConstruction);
        }

        private HnswIndexOptions(int m, int efConstruction) {
            super("hnsw");
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HnswIndexOptions that = (HnswIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, m, efConstruction);
        }

        @Override
        public String toString() {
            return "{type=" + type + ", m=" + m + ", ef_construction=" + efConstruction + " }";
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    public static final class DenseVectorFieldType extends SimpleMappedFieldType {
        private final int dims;
        private final boolean indexed;
        private final VectorSimilarity similarity;
        private final Version indexVersionCreated;

        public DenseVectorFieldType(
            String name,
            Version indexVersionCreated,
            int dims,
            boolean indexed,
            VectorSimilarity similarity,
            Map<String, String> meta
        ) {
            super(name, indexed, false, indexed == false, TextSearchInfo.NONE, meta);
            this.dims = dims;
            this.indexed = indexed;
            this.similarity = similarity;
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
                "Field [" + name() + "] of type [" + typeName() + "] doesn't support docvalue_fields or aggregations"
            );
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new VectorIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, indexVersionCreated, dims, indexed);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        public KnnVectorQuery createKnnQuery(float[] queryVector, int numCands, Query filter) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }

            if (queryVector.length != dims) {
                throw new IllegalArgumentException(
                    "the query vector has a different dimension [" + queryVector.length + "] " + "than the index vectors [" + dims + "]"
                );
            }

            if (similarity == VectorSimilarity.dot_product || similarity == VectorSimilarity.cosine) {
                float squaredMagnitude = 0.0f;
                for (float e : queryVector) {
                    squaredMagnitude += e * e;
                }
                checkVectorMagnitude(queryVector, squaredMagnitude);
            }
            return new KnnVectorQuery(name(), queryVector, numCands, filter);
        }

        private void checkVectorMagnitude(float[] vector, float squaredMagnitude) {
            StringBuilder errorBuilder = null;
            if (similarity == VectorSimilarity.dot_product && Math.abs(squaredMagnitude - 1.0f) > 1e-4f) {
                errorBuilder = new StringBuilder(
                    "The [" + VectorSimilarity.dot_product.name() + "] similarity can " + "only be used with unit-length vectors."
                );
            } else if (similarity == VectorSimilarity.cosine && Math.sqrt(squaredMagnitude) == 0.0f) {
                errorBuilder = new StringBuilder(
                    "The [" + VectorSimilarity.cosine.name() + "] similarity does not support vectors with zero magnitude."
                );
            }

            if (errorBuilder != null) {
                // Include the first five elements of the invalid vector in the error message
                errorBuilder.append(" Preview of invalid vector: [");
                for (int i = 0; i < Math.min(5, vector.length); i++) {
                    if (i > 0) {
                        errorBuilder.append(", ");
                    }
                    errorBuilder.append(vector[i]);
                }
                if (vector.length >= 5) {
                    errorBuilder.append(", ...");
                }
                errorBuilder.append("]");
                throw new IllegalArgumentException(errorBuilder.toString());
            }
        }
    }

    private final int dims;
    private final boolean indexed;
    private final VectorSimilarity similarity;
    private final IndexOptions indexOptions;
    private final Version indexCreatedVersion;

    private DenseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        int dims,
        boolean indexed,
        VectorSimilarity similarity,
        IndexOptions indexOptions,
        Version indexCreatedVersion,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.dims = dims;
        this.indexed = indexed;
        this.similarity = similarity;
        this.indexOptions = indexOptions;
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
            throw new IllegalArgumentException(
                "Field ["
                    + name()
                    + "] of type ["
                    + typeName()
                    + "] doesn't not support indexing multiple values for the same field in the same document"
            );
        }

        Field field = fieldType().indexed ? parseKnnVector(context) : parseBinaryDocValuesVector(context);
        context.doc().addWithKey(fieldType().name(), field);
    }

    private Field parseKnnVector(DocumentParserContext context) throws IOException {
        float[] vector = new float[dims];
        float squaredMagnitude = 0.0f;
        int index = 0;
        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
            checkDimensionExceeded(index, context);
            ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());

            float value = context.parser().floatValue(true);
            vector[index++] = value;
            squaredMagnitude += value * value;
        }
        checkDimensionMatches(index, context);
        fieldType().checkVectorMagnitude(vector, squaredMagnitude);
        return new KnnVectorField(fieldType().name(), vector, similarity.function);
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
            throw new IllegalArgumentException(
                "The ["
                    + typeName()
                    + "] field ["
                    + name()
                    + "] in doc ["
                    + context.documentDescription()
                    + "] has more dimensions "
                    + "than defined in the mapping ["
                    + dims
                    + "]"
            );
        }
    }

    private void checkDimensionMatches(int index, DocumentParserContext context) {
        if (index != dims) {
            throw new IllegalArgumentException(
                "The ["
                    + typeName()
                    + "] field ["
                    + name()
                    + "] in doc ["
                    + context.documentDescription()
                    + "] has a different number of dimensions "
                    + "["
                    + index
                    + "] than defined in the mapping ["
                    + dims
                    + "]"
            );
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

    @Override
    public void doValidate(MappingLookup mappers) {
        if (indexed && mappers.nestedLookup().getNestedParent(name()) != null) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields cannot be indexed if they're" + " within [nested] mappings");
        }
    }

    private static IndexOptions parseIndexOptions(String fieldName, Object propNode) {
        @SuppressWarnings("unchecked")
        Map<String, ?> indexOptionsMap = (Map<String, ?>) propNode;
        Object typeNode = indexOptionsMap.remove("type");
        if (typeNode == null) {
            throw new MapperParsingException("[index_options] requires field [type] to be configured");
        }
        String type = XContentMapValues.nodeStringValue(typeNode);
        if (type.equals("hnsw")) {
            return HnswIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else {
            throw new MapperParsingException("Unknown vector index options type [" + type + "] for field [" + fieldName + "]");
        }
    }

    /**
     * @return the custom kNN vectors format that is configured for this field or
     * {@code null} if the default format should be used.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField() {
        if (indexOptions == null) {
            return null; // use default format
        } else {
            HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) indexOptions;
            return new Lucene94HnswVectorsFormat(hnswIndexOptions.m, hnswIndexOptions.efConstruction);
        }
    }
}
