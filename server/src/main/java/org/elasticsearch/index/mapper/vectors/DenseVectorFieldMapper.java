/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ByteKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.FloatKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParser;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.codec.ESLucene99HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.vectors.ProfilingDiversifyingChildrenByteKnnVectorQuery;
import org.elasticsearch.search.vectors.ProfilingDiversifyingChildrenFloatKnnVectorQuery;
import org.elasticsearch.search.vectors.ProfilingKnnByteVectorQuery;
import org.elasticsearch.search.vectors.ProfilingKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {
    public static final String COSINE_MAGNITUDE_FIELD_SUFFIX = "._magnitude";
    private static final float EPS = 1e-4f;

    static boolean isNotUnitVector(float magnitude) {
        return Math.abs(magnitude - 1.0f) > EPS;
    }

    public static final IndexVersion MAGNITUDE_STORED_INDEX_VERSION = IndexVersions.V_7_5_0;
    public static final IndexVersion INDEXED_BY_DEFAULT_INDEX_VERSION = IndexVersions.FIRST_DETACHED_INDEX_VERSION;
    public static final IndexVersion NORMALIZE_COSINE = IndexVersions.NORMALIZED_VECTOR_COSINE;
    public static final IndexVersion LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION = IndexVersions.V_8_9_0;

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 4096; // maximum allowed number of dimensions

    public static short MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING = 128; // minimum number of dims for floats to be dynamically mapped to vector
    public static final int MAGNITUDE_BYTES = 4;

    private static DenseVectorFieldMapper toType(FieldMapper in) {
        return (DenseVectorFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<ElementType> elementType = new Parameter<>("element_type", false, () -> ElementType.FLOAT, (n, c, o) -> {
            ElementType elementType = namesToElementType.get((String) o);
            if (elementType == null) {
                throw new MapperParsingException("invalid element_type [" + o + "]; available types are " + namesToElementType.keySet());
            }
            return elementType;
        }, m -> toType(m).fieldType().elementType, XContentBuilder::field, Objects::toString);

        // This is defined as updatable because it can be updated once, from [null] to a valid dim size,
        // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
        private final Parameter<Integer> dims = new Parameter<>("dims", true, () -> null, (n, c, o) -> {
            if (o instanceof Integer == false) {
                throw new MapperParsingException("Property [dims] on field [" + n + "] must be an integer but got [" + o + "]");
            }
            int dims = XContentMapValues.nodeIntegerValue(o);
            if (dims < 1 || dims > MAX_DIMS_COUNT) {
                throw new MapperParsingException(
                    "The number of dimensions for field ["
                        + n
                        + "] should be in the range [1, "
                        + MAX_DIMS_COUNT
                        + "] but was ["
                        + dims
                        + "]"
                );
            }
            return dims;
        }, m -> toType(m).fieldType().dims, XContentBuilder::field, Object::toString).setSerializerCheck((id, ic, v) -> v != null)
            .setMergeValidator((previous, current, c) -> previous == null || Objects.equals(previous, current));
        private final Parameter<VectorSimilarity> similarity;
        private final Parameter<IndexOptions> indexOptions = new Parameter<>(
            "index_options",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : parseIndexOptions(n, o),
            m -> toType(m).indexOptions,
            XContentBuilder::field,
            Objects::toString
        ).setSerializerCheck((id, ic, v) -> v != null);
        private final Parameter<Boolean> indexed;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final IndexVersion indexVersionCreated;

        public Builder(String name, IndexVersion indexVersionCreated) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
            final boolean indexedByDefault = indexVersionCreated.onOrAfter(INDEXED_BY_DEFAULT_INDEX_VERSION);
            this.indexed = Parameter.indexParam(m -> toType(m).fieldType().indexed, indexedByDefault);
            if (indexedByDefault) {
                // Only serialize on newer index versions to prevent breaking existing indices when upgrading
                this.indexed.alwaysSerialize();
            }
            this.similarity = Parameter.enumParam(
                "similarity",
                false,
                m -> toType(m).fieldType().similarity,
                (Supplier<VectorSimilarity>) () -> indexedByDefault && indexed.getValue() ? VectorSimilarity.COSINE : null,
                VectorSimilarity.class
            ).acceptsNull().setSerializerCheck((id, ic, v) -> v != null);
            this.indexed.addValidator(v -> {
                if (v) {
                    if (similarity.getValue() == null) {
                        throw new IllegalArgumentException("Field [index] requires field [similarity] to be configured and not null");
                    }
                } else {
                    if (similarity.isConfigured() && similarity.getValue() != null) {
                        throw new IllegalArgumentException(
                            "Field [similarity] can only be specified for a field of type [dense_vector] when it is indexed"
                        );
                    }
                    if (indexOptions.isConfigured() && indexOptions.getValue() != null) {
                        throw new IllegalArgumentException(
                            "Field [index_options] can only be specified for a field of type [dense_vector] when it is indexed"
                        );
                    }
                }
            });
            this.indexOptions.addValidator(v -> {
                if (v instanceof Int8HnswIndexOptions && elementType.getValue() == ElementType.BYTE) {
                    throw new IllegalArgumentException(
                        "[element_type] cannot be [byte] when using index type [" + VectorIndexType.INT8_HNSW.name + "]"
                    );
                }
            });
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { elementType, dims, indexed, similarity, indexOptions, meta };
        }

        @Override
        public DenseVectorFieldMapper build(MapperBuilderContext context) {
            return new DenseVectorFieldMapper(
                name,
                new DenseVectorFieldType(
                    context.buildFullName(name),
                    indexVersionCreated,
                    elementType.getValue(),
                    dims.getValue(),
                    indexed.getValue(),
                    similarity.getValue(),
                    meta.getValue()
                ),
                indexOptions.getValue(),
                indexVersionCreated,
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    public enum ElementType {

        BYTE(1) {

            @Override
            public String toString() {
                return "byte";
            }

            @Override
            public void writeValue(ByteBuffer byteBuffer, float value) {
                byteBuffer.put((byte) value);
            }

            @Override
            public void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException {
                b.value(byteBuffer.get());
            }

            @Override
            KnnByteVectorField createKnnVectorField(String name, byte[] vector, VectorSimilarityFunction function) {
                if (vector == null) {
                    throw new IllegalArgumentException("vector value must not be null");
                }
                FieldType denseVectorFieldType = new FieldType();
                denseVectorFieldType.setVectorAttributes(vector.length, VectorEncoding.BYTE, function);
                denseVectorFieldType.freeze();
                return new KnnByteVectorField(name, vector, denseVectorFieldType);
            }

            @Override
            KnnFloatVectorField createKnnVectorField(String name, float[] vector, VectorSimilarityFunction function) {
                throw new IllegalArgumentException("cannot create a float vector field from byte");
            }

            @Override
            IndexFieldData.Builder fielddataBuilder(DenseVectorFieldType denseVectorFieldType, FieldDataContext fieldDataContext) {
                return new VectorIndexFieldData.Builder(
                    denseVectorFieldType.name(),
                    CoreValuesSourceType.KEYWORD,
                    denseVectorFieldType.indexVersionCreated,
                    this,
                    denseVectorFieldType.dims,
                    denseVectorFieldType.indexed,
                    r -> r
                );
            }

            @Override
            public void checkVectorBounds(float[] vector) {
                checkNanAndInfinite(vector);

                StringBuilder errorBuilder = null;

                for (int index = 0; index < vector.length; ++index) {
                    float value = vector[index];

                    if (value % 1.0f != 0.0f) {
                        errorBuilder = new StringBuilder(
                            "element_type ["
                                + this
                                + "] vectors only support non-decimal values but found decimal value ["
                                + value
                                + "] at dim ["
                                + index
                                + "];"
                        );
                        break;
                    }

                    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                        errorBuilder = new StringBuilder(
                            "element_type ["
                                + this
                                + "] vectors only support integers between ["
                                + Byte.MIN_VALUE
                                + ", "
                                + Byte.MAX_VALUE
                                + "] but found ["
                                + value
                                + "] at dim ["
                                + index
                                + "];"
                        );
                        break;
                    }
                }

                if (errorBuilder != null) {
                    throw new IllegalArgumentException(appendErrorElements(errorBuilder, vector).toString());
                }
            }

            @Override
            void checkVectorMagnitude(
                VectorSimilarity similarity,
                Function<StringBuilder, StringBuilder> appender,
                float squaredMagnitude
            ) {
                StringBuilder errorBuilder = null;

                if (similarity == VectorSimilarity.COSINE && Math.sqrt(squaredMagnitude) == 0.0f) {
                    errorBuilder = new StringBuilder(
                        "The [" + VectorSimilarity.COSINE + "] similarity does not support vectors with zero magnitude."
                    );
                }

                if (errorBuilder != null) {
                    throw new IllegalArgumentException(appender.apply(errorBuilder).toString());
                }
            }

            @Override
            public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                int index = 0;
                byte[] vector = new byte[fieldMapper.fieldType().dims];
                float squaredMagnitude = 0;
                for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                    fieldMapper.checkDimensionExceeded(index, context);
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                    final int value;
                    if (context.parser().numberType() != XContentParser.NumberType.INT) {
                        float floatValue = context.parser().floatValue(true);
                        if (floatValue % 1.0f != 0.0f) {
                            throw new IllegalArgumentException(
                                "element_type ["
                                    + this
                                    + "] vectors only support non-decimal values but found decimal value ["
                                    + floatValue
                                    + "] at dim ["
                                    + index
                                    + "];"
                            );
                        }
                        value = (int) floatValue;
                    } else {
                        value = context.parser().intValue(true);
                    }
                    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                        throw new IllegalArgumentException(
                            "element_type ["
                                + this
                                + "] vectors only support integers between ["
                                + Byte.MIN_VALUE
                                + ", "
                                + Byte.MAX_VALUE
                                + "] but found ["
                                + value
                                + "] at dim ["
                                + index
                                + "];"
                        );
                    }
                    vector[index++] = (byte) value;
                    squaredMagnitude += value * value;
                }
                fieldMapper.checkDimensionMatches(index, context);
                checkVectorMagnitude(fieldMapper.fieldType().similarity, errorByteElementsAppender(vector), squaredMagnitude);
                Field field = createKnnVectorField(
                    fieldMapper.fieldType().name(),
                    vector,
                    fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, this)
                );
                context.doc().addWithKey(fieldMapper.fieldType().name(), field);
            }

            @Override
            double parseKnnVectorToByteBuffer(DocumentParserContext context, DenseVectorFieldMapper fieldMapper, ByteBuffer byteBuffer)
                throws IOException {
                double dotProduct = 0f;
                int index = 0;
                for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                    fieldMapper.checkDimensionExceeded(index, context);
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                    int value = context.parser().intValue(true);
                    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                        throw new IllegalArgumentException(
                            "element_type ["
                                + this
                                + "] vectors only support integers between ["
                                + Byte.MIN_VALUE
                                + ", "
                                + Byte.MAX_VALUE
                                + "] but found ["
                                + value
                                + "] at dim ["
                                + index
                                + "];"
                        );
                    }
                    byteBuffer.put((byte) value);
                    dotProduct += value * value;
                    index++;
                }
                fieldMapper.checkDimensionMatches(index, context);
                return dotProduct;
            }

            @Override
            ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
                return ByteBuffer.wrap(new byte[numBytes]);
            }
        },

        FLOAT(4) {

            @Override
            public String toString() {
                return "float";
            }

            @Override
            public void writeValue(ByteBuffer byteBuffer, float value) {
                byteBuffer.putFloat(value);
            }

            @Override
            public void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException {
                b.value(byteBuffer.getFloat());
            }

            @Override
            KnnFloatVectorField createKnnVectorField(String name, float[] vector, VectorSimilarityFunction function) {
                if (vector == null) {
                    throw new IllegalArgumentException("vector value must not be null");
                }
                FieldType denseVectorFieldType = new FieldType();
                denseVectorFieldType.setVectorAttributes(vector.length, VectorEncoding.FLOAT32, function);
                denseVectorFieldType.freeze();
                return new KnnFloatVectorField(name, vector, denseVectorFieldType);
            }

            @Override
            KnnByteVectorField createKnnVectorField(String name, byte[] vector, VectorSimilarityFunction function) {
                throw new IllegalArgumentException("cannot create a byte vector field from float");
            }

            @Override
            IndexFieldData.Builder fielddataBuilder(DenseVectorFieldType denseVectorFieldType, FieldDataContext fieldDataContext) {
                return new VectorIndexFieldData.Builder(
                    denseVectorFieldType.name(),
                    CoreValuesSourceType.KEYWORD,
                    denseVectorFieldType.indexVersionCreated,
                    this,
                    denseVectorFieldType.dims,
                    denseVectorFieldType.indexed,
                    denseVectorFieldType.indexVersionCreated.onOrAfter(NORMALIZE_COSINE)
                        && denseVectorFieldType.indexed
                        && denseVectorFieldType.similarity.equals(VectorSimilarity.COSINE) ? r -> new FilterLeafReader(r) {
                            @Override
                            public CacheHelper getCoreCacheHelper() {
                                return r.getCoreCacheHelper();
                            }

                            @Override
                            public CacheHelper getReaderCacheHelper() {
                                return r.getReaderCacheHelper();
                            }

                            @Override
                            public FloatVectorValues getFloatVectorValues(String fieldName) throws IOException {
                                FloatVectorValues values = in.getFloatVectorValues(fieldName);
                                if (values == null) {
                                    return null;
                                }
                                return new DenormalizedCosineFloatVectorValues(
                                    values,
                                    in.getNumericDocValues(fieldName + COSINE_MAGNITUDE_FIELD_SUFFIX)
                                );
                            }
                        } : r -> r
                );
            }

            @Override
            public void checkVectorBounds(float[] vector) {
                checkNanAndInfinite(vector);
            }

            @Override
            void checkVectorMagnitude(
                VectorSimilarity similarity,
                Function<StringBuilder, StringBuilder> appender,
                float squaredMagnitude
            ) {
                StringBuilder errorBuilder = null;

                if (Float.isNaN(squaredMagnitude) || Float.isInfinite(squaredMagnitude)) {
                    errorBuilder = new StringBuilder(
                        "NaN or Infinite magnitude detected, this usually means the vector values are too extreme to fit within a float."
                    );
                }
                if (errorBuilder != null) {
                    throw new IllegalArgumentException(appender.apply(errorBuilder).toString());
                }

                if (similarity == VectorSimilarity.DOT_PRODUCT && isNotUnitVector(squaredMagnitude)) {
                    errorBuilder = new StringBuilder(
                        "The [" + VectorSimilarity.DOT_PRODUCT + "] similarity can only be used with unit-length vectors."
                    );
                } else if (similarity == VectorSimilarity.COSINE && Math.sqrt(squaredMagnitude) == 0.0f) {
                    errorBuilder = new StringBuilder(
                        "The [" + VectorSimilarity.COSINE + "] similarity does not support vectors with zero magnitude."
                    );
                }

                if (errorBuilder != null) {
                    throw new IllegalArgumentException(appender.apply(errorBuilder).toString());
                }
            }

            @Override
            public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                int index = 0;
                float[] vector = new float[fieldMapper.fieldType().dims];
                float squaredMagnitude = 0;
                for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                    fieldMapper.checkDimensionExceeded(index, context);
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());

                    float value = context.parser().floatValue(true);
                    vector[index++] = value;
                    squaredMagnitude += value * value;
                }
                fieldMapper.checkDimensionMatches(index, context);
                checkVectorBounds(vector);
                checkVectorMagnitude(fieldMapper.fieldType().similarity, errorFloatElementsAppender(vector), squaredMagnitude);
                if (fieldMapper.indexCreatedVersion.onOrAfter(NORMALIZE_COSINE)
                    && fieldMapper.fieldType().similarity.equals(VectorSimilarity.COSINE)
                    && isNotUnitVector(squaredMagnitude)) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    for (int i = 0; i < vector.length; i++) {
                        vector[i] /= length;
                    }
                    final String fieldName = fieldMapper.fieldType().name() + COSINE_MAGNITUDE_FIELD_SUFFIX;
                    Field magnitudeField = new FloatDocValuesField(fieldName, length);
                    context.doc().addWithKey(fieldName, magnitudeField);
                }
                Field field = createKnnVectorField(
                    fieldMapper.fieldType().name(),
                    vector,
                    fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, this)
                );
                context.doc().addWithKey(fieldMapper.fieldType().name(), field);
            }

            @Override
            double parseKnnVectorToByteBuffer(DocumentParserContext context, DenseVectorFieldMapper fieldMapper, ByteBuffer byteBuffer)
                throws IOException {
                double dotProduct = 0f;
                int index = 0;
                float[] vector = new float[fieldMapper.fieldType().dims];
                for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                    fieldMapper.checkDimensionExceeded(index, context);
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                    float value = context.parser().floatValue(true);
                    vector[index] = value;
                    byteBuffer.putFloat(value);
                    dotProduct += value * value;
                    index++;
                }
                fieldMapper.checkDimensionMatches(index, context);
                checkVectorBounds(vector);
                return dotProduct;
            }

            @Override
            ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
                return indexVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)
                    ? ByteBuffer.wrap(new byte[numBytes]).order(ByteOrder.LITTLE_ENDIAN)
                    : ByteBuffer.wrap(new byte[numBytes]);
            }
        };

        final int elementBytes;

        ElementType(int elementBytes) {
            this.elementBytes = elementBytes;
        }

        public abstract void writeValue(ByteBuffer byteBuffer, float value);

        public abstract void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException;

        abstract KnnFloatVectorField createKnnVectorField(String name, float[] vector, VectorSimilarityFunction function);

        abstract KnnByteVectorField createKnnVectorField(String name, byte[] vector, VectorSimilarityFunction function);

        abstract IndexFieldData.Builder fielddataBuilder(DenseVectorFieldType denseVectorFieldType, FieldDataContext fieldDataContext);

        abstract void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException;

        abstract double parseKnnVectorToByteBuffer(DocumentParserContext context, DenseVectorFieldMapper fieldMapper, ByteBuffer byteBuffer)
            throws IOException;

        abstract ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes);

        public abstract void checkVectorBounds(float[] vector);

        abstract void checkVectorMagnitude(
            VectorSimilarity similarity,
            Function<StringBuilder, StringBuilder> errorElementsAppender,
            float squaredMagnitude
        );

        int parseDimensionCount(DocumentParserContext context) throws IOException {
            int index = 0;
            for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                index++;
            }
            return index;
        }

        void checkNanAndInfinite(float[] vector) {
            StringBuilder errorBuilder = null;

            for (int index = 0; index < vector.length; ++index) {
                float value = vector[index];

                if (Float.isNaN(value)) {
                    errorBuilder = new StringBuilder(
                        "element_type [" + this + "] vectors do not support NaN values but found [" + value + "] at dim [" + index + "];"
                    );
                    break;
                }

                if (Float.isInfinite(value)) {
                    errorBuilder = new StringBuilder(
                        "element_type ["
                            + this
                            + "] vectors do not support infinite values but found ["
                            + value
                            + "] at dim ["
                            + index
                            + "];"
                    );
                    break;
                }
            }

            if (errorBuilder != null) {
                throw new IllegalArgumentException(appendErrorElements(errorBuilder, vector).toString());
            }
        }

        static StringBuilder appendErrorElements(StringBuilder errorBuilder, float[] vector) {
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
            return errorBuilder;
        }

        static StringBuilder appendErrorElements(StringBuilder errorBuilder, byte[] vector) {
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
            return errorBuilder;
        }

        static Function<StringBuilder, StringBuilder> errorFloatElementsAppender(float[] vector) {
            return sb -> appendErrorElements(sb, vector);
        }

        static Function<StringBuilder, StringBuilder> errorByteElementsAppender(byte[] vector) {
            return sb -> appendErrorElements(sb, vector);
        }
    }

    static final Map<String, ElementType> namesToElementType = Map.of(
        ElementType.BYTE.toString(),
        ElementType.BYTE,
        ElementType.FLOAT.toString(),
        ElementType.FLOAT
    );

    enum VectorSimilarity {
        L2_NORM {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE, FLOAT -> 1f / (1f + similarity * similarity);
                };
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType) {
                return VectorSimilarityFunction.EUCLIDEAN;
            }
        },
        COSINE {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE, FLOAT -> (1 + similarity) / 2f;
                };
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType) {
                return indexVersion.onOrAfter(NORMALIZE_COSINE) && ElementType.FLOAT.equals(elementType)
                    ? VectorSimilarityFunction.DOT_PRODUCT
                    : VectorSimilarityFunction.COSINE;
            }
        },
        DOT_PRODUCT {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE -> 0.5f + similarity / (float) (dim * (1 << 15));
                    case FLOAT -> (1 + similarity) / 2f;
                };
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType) {
                return VectorSimilarityFunction.DOT_PRODUCT;
            }
        },
        MAX_INNER_PRODUCT {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE, FLOAT -> similarity < 0 ? 1 / (1 + -1 * similarity) : similarity + 1;
                };
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType) {
                return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
            }
        };

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        abstract float score(float similarity, ElementType elementType, int dim);

        public abstract VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType);
    }

    private abstract static class IndexOptions implements ToXContent {
        final String type;

        IndexOptions(String type) {
            this.type = type;
        }

        abstract KnnVectorsFormat getVectorsFormat();
    }

    private enum VectorIndexType {
        HNSW("hnsw") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                if (mNode == null) {
                    mNode = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
                }
                if (efConstructionNode == null) {
                    efConstructionNode = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
                }
                int m = XContentMapValues.nodeIntegerValue(mNode);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode);
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new HnswIndexOptions(m, efConstruction);
            }
        },
        INT8_HNSW("int8_hnsw") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                if (mNode == null) {
                    mNode = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
                }
                if (efConstructionNode == null) {
                    efConstructionNode = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
                }
                int m = XContentMapValues.nodeIntegerValue(mNode);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode);
                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int8HnswIndexOptions(m, efConstruction, confidenceInterval);
            }
        };

        static Optional<VectorIndexType> fromString(String type) {
            return Stream.of(VectorIndexType.values()).filter(vectorIndexType -> vectorIndexType.name.equals(type)).findFirst();
        }

        private final String name;

        VectorIndexType(String name) {
            this.name = name;
        }

        abstract IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap);
    }

    private static class Int8HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;
        private final Float confidenceInterval;

        private Int8HnswIndexOptions(int m, int efConstruction, Float confidenceInterval) {
            super("int8_hnsw");
            this.m = m;
            this.efConstruction = efConstruction;
            this.confidenceInterval = confidenceInterval;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat() {
            return new ESLucene99HnswScalarQuantizedVectorsFormat(m, efConstruction, 1, confidenceInterval, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            if (confidenceInterval != null) {
                builder.field("confidence_interval", confidenceInterval);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Int8HnswIndexOptions that = (Int8HnswIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction && Objects.equals(confidenceInterval, that.confidenceInterval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m, efConstruction, confidenceInterval);
        }

        @Override
        public String toString() {
            return "{type="
                + type
                + ", m="
                + m
                + ", ef_construction="
                + efConstruction
                + ", confidence_interval="
                + confidenceInterval
                + "}";
        }
    }

    private static class HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;

        private HnswIndexOptions(int m, int efConstruction) {
            super("hnsw");
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat() {
            return new Lucene99HnswVectorsFormat(m, efConstruction, 1, null);
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
            return "{type=" + type + ", m=" + m + ", ef_construction=" + efConstruction + "}";
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    public static final class DenseVectorFieldType extends SimpleMappedFieldType {
        private final ElementType elementType;
        private final Integer dims;
        private final boolean indexed;
        private final VectorSimilarity similarity;
        private final IndexVersion indexVersionCreated;

        public DenseVectorFieldType(
            String name,
            IndexVersion indexVersionCreated,
            ElementType elementType,
            Integer dims,
            boolean indexed,
            VectorSimilarity similarity,
            Map<String, String> meta
        ) {
            super(name, indexed, false, indexed == false, TextSearchInfo.NONE, meta);
            this.elementType = elementType;
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
            return elementType.fielddataBuilder(this, fieldDataContext);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        public Query createKnnQuery(
            byte[] queryVector,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter
        ) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }

            if (queryVector.length != dims) {
                throw new IllegalArgumentException(
                    "the query vector has a different dimension [" + queryVector.length + "] than the index vectors [" + dims + "]"
                );
            }

            if (elementType != ElementType.BYTE) {
                throw new IllegalArgumentException(
                    "only [" + ElementType.BYTE + "] elements are supported when querying field [" + name() + "]"
                );
            }

            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorByteElementsAppender(queryVector), squaredMagnitude);
            }
            Query knnQuery = parentFilter != null
                ? new DiversifyingChildrenByteKnnVectorQuery(name(), queryVector, filter, numCands, parentFilter)
                : new KnnByteVectorQuery(name(), queryVector, numCands, filter);
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, elementType, dims)
                );
            }
            return knnQuery;
        }

        public Query createExactKnnQuery(float[] queryVector) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }
            if (queryVector.length != dims) {
                throw new IllegalArgumentException(
                    "the query vector has a different dimension [" + queryVector.length + "] than the index vectors [" + dims + "]"
                );
            }
            elementType.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorFloatElementsAppender(queryVector), squaredMagnitude);
                if (similarity == VectorSimilarity.COSINE
                    && ElementType.FLOAT.equals(elementType)
                    && indexVersionCreated.onOrAfter(NORMALIZE_COSINE)
                    && isNotUnitVector(squaredMagnitude)) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    queryVector = Arrays.copyOf(queryVector, queryVector.length);
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] /= length;
                    }
                }
            }
            VectorSimilarityFunction vectorSimilarityFunction = similarity.vectorSimilarityFunction(indexVersionCreated, elementType);
            return switch (elementType) {
                case BYTE -> {
                    byte[] bytes = new byte[queryVector.length];
                    for (int i = 0; i < queryVector.length; i++) {
                        bytes[i] = (byte) queryVector[i];
                    }
                    yield new BooleanQuery.Builder().add(new FieldExistsQuery(name()), BooleanClause.Occur.FILTER)
                        .add(
                            new FunctionQuery(
                                new ByteVectorSimilarityFunction(
                                    vectorSimilarityFunction,
                                    new ByteKnnVectorFieldSource(name()),
                                    new ConstKnnByteVectorValueSource(bytes)
                                )
                            ),
                            BooleanClause.Occur.SHOULD
                        )
                        .build();
                }
                case FLOAT -> new BooleanQuery.Builder().add(new FieldExistsQuery(name()), BooleanClause.Occur.FILTER)
                    .add(
                        new FunctionQuery(
                            new FloatVectorSimilarityFunction(
                                vectorSimilarityFunction,
                                new FloatKnnVectorFieldSource(name()),
                                new ConstKnnFloatValueSource(queryVector)
                            )
                        ),
                        BooleanClause.Occur.SHOULD
                    )
                    .build();
            };
        }

        public Query createKnnQuery(
            float[] queryVector,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter
        ) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }

            if (queryVector.length != dims) {
                throw new IllegalArgumentException(
                    "the query vector has a different dimension [" + queryVector.length + "] than the index vectors [" + dims + "]"
                );
            }
            elementType.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorFloatElementsAppender(queryVector), squaredMagnitude);
                if (similarity == VectorSimilarity.COSINE
                    && ElementType.FLOAT.equals(elementType)
                    && indexVersionCreated.onOrAfter(NORMALIZE_COSINE)
                    && isNotUnitVector(squaredMagnitude)) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    queryVector = Arrays.copyOf(queryVector, queryVector.length);
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] /= length;
                    }
                }
            }
            Query knnQuery = switch (elementType) {
                case BYTE -> {
                    byte[] bytes = new byte[queryVector.length];
                    for (int i = 0; i < queryVector.length; i++) {
                        bytes[i] = (byte) queryVector[i];
                    }
                    yield parentFilter != null
                        ? new ProfilingDiversifyingChildrenByteKnnVectorQuery(name(), bytes, filter, numCands, parentFilter)
                        : new ProfilingKnnByteVectorQuery(name(), bytes, numCands, filter);
                }
                case FLOAT -> parentFilter != null
                    ? new ProfilingDiversifyingChildrenFloatKnnVectorQuery(name(), queryVector, filter, numCands, parentFilter)
                    : new ProfilingKnnFloatVectorQuery(name(), queryVector, numCands, filter);

            };

            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, elementType, dims)
                );
            }
            return knnQuery;
        }

        VectorSimilarity getSimilarity() {
            return similarity;
        }

        int getVectorDimensions() {
            return dims;
        }

        ElementType getElementType() {
            return elementType;
        }
    }

    private final IndexOptions indexOptions;
    private final IndexVersion indexCreatedVersion;

    private DenseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        IndexOptions indexOptions,
        IndexVersion indexCreatedVersion,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
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
        if (Token.VALUE_NULL == context.parser().currentToken()) {
            return;
        }
        if (fieldType().dims == null) {
            int dims = fieldType().elementType.parseDimensionCount(context);
            DenseVectorFieldType updatedDenseVectorFieldType = new DenseVectorFieldType(
                fieldType().name(),
                indexCreatedVersion,
                fieldType().elementType,
                dims,
                fieldType().indexed,
                fieldType().similarity,
                fieldType().meta()
            );
            Mapper update = new DenseVectorFieldMapper(
                simpleName(),
                updatedDenseVectorFieldType,
                indexOptions,
                indexCreatedVersion,
                multiFields(),
                copyTo
            );
            context.addDynamicMapper(update);
            return;
        }
        if (fieldType().indexed) {
            parseKnnVectorAndIndex(context);
        } else {
            parseBinaryDocValuesVectorAndIndex(context);
        }
    }

    private void parseKnnVectorAndIndex(DocumentParserContext context) throws IOException {
        fieldType().elementType.parseKnnVectorAndIndex(context, this);
    }

    private void parseBinaryDocValuesVectorAndIndex(DocumentParserContext context) throws IOException {
        // encode array of floats as array of integers and store into buf
        // this code is here and not in the VectorEncoderDecoder so not to create extra arrays
        int dims = fieldType().dims;
        ElementType elementType = fieldType().elementType;
        int numBytes = indexCreatedVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)
            ? dims * elementType.elementBytes + MAGNITUDE_BYTES
            : dims * elementType.elementBytes;

        ByteBuffer byteBuffer = elementType.createByteBuffer(indexCreatedVersion, numBytes);
        double dotProduct = elementType.parseKnnVectorToByteBuffer(context, this, byteBuffer);
        if (indexCreatedVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        Field field = new BinaryDocValuesField(fieldType().name(), new BytesRef(byteBuffer.array()));
        context.doc().addWithKey(fieldType().name(), field);
    }

    private void checkDimensionExceeded(int index, DocumentParserContext context) {
        if (index >= fieldType().dims) {
            throw new IllegalArgumentException(
                "The ["
                    + typeName()
                    + "] field ["
                    + name()
                    + "] in doc ["
                    + context.documentDescription()
                    + "] has more dimensions "
                    + "than defined in the mapping ["
                    + fieldType().dims
                    + "]"
            );
        }
    }

    private void checkDimensionMatches(int index, DocumentParserContext context) {
        if (index != fieldType().dims) {
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
                    + fieldType().dims
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

    private static IndexOptions parseIndexOptions(String fieldName, Object propNode) {
        @SuppressWarnings("unchecked")
        Map<String, ?> indexOptionsMap = (Map<String, ?>) propNode;
        Object typeNode = indexOptionsMap.remove("type");
        if (typeNode == null) {
            throw new MapperParsingException("[index_options] requires field [type] to be configured");
        }
        String type = XContentMapValues.nodeStringValue(typeNode);
        return VectorIndexType.fromString(type)
            .orElseThrow(() -> new MapperParsingException("Unknown vector index options type [" + type + "] for field [" + fieldName + "]"))
            .parseIndexOptions(fieldName, indexOptionsMap);
    }

    /**
     * @return the custom kNN vectors format that is configured for this field or
     * {@code null} if the default format should be used.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(KnnVectorsFormat defaultFormat) {
        final KnnVectorsFormat format;
        if (indexOptions == null) {
            format = defaultFormat;
        } else {
            format = indexOptions.getVectorsFormat();
        }
        // It's legal to reuse the same format name as this is the same on-disk format.
        return new KnnVectorsFormat(format.getName()) {
            @Override
            public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                return format.fieldsWriter(state);
            }

            @Override
            public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                return format.fieldsReader(state);
            }

            @Override
            public int getMaxDimensions(String fieldName) {
                return MAX_DIMS_COUNT;
            }

            @Override
            public String toString() {
                return format.toString();
            }
        };
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + name() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        if (fieldType().indexed) {
            return new IndexedSyntheticFieldLoader(indexCreatedVersion, fieldType().similarity);
        }
        return new DocValuesSyntheticFieldLoader(indexCreatedVersion);
    }

    private class IndexedSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private FloatVectorValues values;
        private ByteVectorValues byteVectorValues;
        private boolean hasValue;
        private boolean hasMagnitude;

        private final IndexVersion indexCreatedVersion;
        private final VectorSimilarity vectorSimilarity;
        private NumericDocValues magnitudeReader;

        private IndexedSyntheticFieldLoader(IndexVersion indexCreatedVersion, VectorSimilarity vectorSimilarity) {
            this.indexCreatedVersion = indexCreatedVersion;
            this.vectorSimilarity = vectorSimilarity;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            values = leafReader.getFloatVectorValues(name());
            if (values != null) {
                if (indexCreatedVersion.onOrAfter(NORMALIZE_COSINE) && VectorSimilarity.COSINE.equals(vectorSimilarity)) {
                    magnitudeReader = leafReader.getNumericDocValues(name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                }
                return docId -> {
                    hasValue = docId == values.advance(docId);
                    hasMagnitude = hasValue && magnitudeReader != null && magnitudeReader.advanceExact(docId);
                    return hasValue;
                };
            }
            byteVectorValues = leafReader.getByteVectorValues(name());
            if (byteVectorValues != null) {
                return docId -> {
                    hasValue = docId == byteVectorValues.advance(docId);
                    return hasValue;
                };
            }
            return null;
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (false == hasValue) {
                return;
            }
            float magnitude = Float.NaN;
            if (hasMagnitude) {
                magnitude = Float.intBitsToFloat((int) magnitudeReader.longValue());
            }
            b.startArray(simpleName());
            if (values != null) {
                for (float v : values.vectorValue()) {
                    if (hasMagnitude) {
                        b.value(v * magnitude);
                    } else {
                        b.value(v);
                    }
                }
            } else if (byteVectorValues != null) {
                byte[] vectorValue = byteVectorValues.vectorValue();
                for (byte value : vectorValue) {
                    b.value(value);
                }
            }
            b.endArray();
        }
    }

    private class DocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private BinaryDocValues values;
        private boolean hasValue;
        private final IndexVersion indexCreatedVersion;

        private DocValuesSyntheticFieldLoader(IndexVersion indexCreatedVersion) {
            this.indexCreatedVersion = indexCreatedVersion;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            values = leafReader.getBinaryDocValues(name());
            if (values == null) {
                return null;
            }
            return docId -> {
                hasValue = docId == values.advance(docId);
                return hasValue;
            };
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (false == hasValue) {
                return;
            }
            b.startArray(simpleName());
            BytesRef ref = values.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
            if (indexCreatedVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)) {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            int dims = fieldType().dims;
            for (int dim = 0; dim < dims; dim++) {
                fieldType().elementType.readAndWriteValue(byteBuffer, b);
            }
            b.endArray();
        }
    }
}
