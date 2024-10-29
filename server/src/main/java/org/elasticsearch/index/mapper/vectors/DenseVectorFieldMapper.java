/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.vectors.ES813FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.ES815BitFlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES815HnswBitVectorsFormat;
import org.elasticsearch.index.codec.vectors.ES816BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.ES816HnswBinaryQuantizedVectorsFormat;
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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.ESDiversifyingChildrenByteKnnVectorQuery;
import org.elasticsearch.search.vectors.ESDiversifyingChildrenFloatKnnVectorQuery;
import org.elasticsearch.search.vectors.ESKnnByteVectorQuery;
import org.elasticsearch.search.vectors.ESKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.VectorData;
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
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.IndexVersions.DEFAULT_DENSE_VECTOR_TO_INT8_HNSW;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {
    public static final String COSINE_MAGNITUDE_FIELD_SUFFIX = "._magnitude";
    private static final float EPS = 1e-3f;
    static final int BBQ_MIN_DIMS = 64;

    public static boolean isNotUnitVector(float magnitude) {
        return Math.abs(magnitude - 1.0f) > EPS;
    }

    public static final NodeFeature INT4_QUANTIZATION = new NodeFeature("mapper.vectors.int4_quantization");
    public static final NodeFeature BIT_VECTORS = new NodeFeature("mapper.vectors.bit_vectors");
    public static final NodeFeature BBQ_FORMAT = new NodeFeature("mapper.vectors.bbq");

    public static final IndexVersion MAGNITUDE_STORED_INDEX_VERSION = IndexVersions.V_7_5_0;
    public static final IndexVersion INDEXED_BY_DEFAULT_INDEX_VERSION = IndexVersions.FIRST_DETACHED_INDEX_VERSION;
    public static final IndexVersion NORMALIZE_COSINE = IndexVersions.NORMALIZED_VECTOR_COSINE;
    public static final IndexVersion DEFAULT_TO_INT8 = DEFAULT_DENSE_VECTOR_TO_INT8_HNSW;
    public static final IndexVersion LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION = IndexVersions.V_8_9_0;

    public static final String CONTENT_TYPE = "dense_vector";
    public static short MAX_DIMS_COUNT = 4096; // maximum allowed number of dimensions
    public static int MAX_DIMS_COUNT_BIT = 4096 * Byte.SIZE; // maximum allowed number of dimensions

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

            return XContentMapValues.nodeIntegerValue(o);
        }, m -> toType(m).fieldType().dims, XContentBuilder::field, Object::toString).setSerializerCheck((id, ic, v) -> v != null)
            .setMergeValidator((previous, current, c) -> previous == null || Objects.equals(previous, current))
            .addValidator(dims -> {
                if (dims == null) {
                    return;
                }
                int maxDims = elementType.getValue() == ElementType.BIT ? MAX_DIMS_COUNT_BIT : MAX_DIMS_COUNT;
                int minDims = elementType.getValue() == ElementType.BIT ? Byte.SIZE : 1;
                if (dims < minDims || dims > maxDims) {
                    throw new MapperParsingException(
                        "The number of dimensions should be in the range [" + minDims + ", " + maxDims + "] but was [" + dims + "]"
                    );
                }
                if (elementType.getValue() == ElementType.BIT) {
                    if (dims % Byte.SIZE != 0) {
                        throw new MapperParsingException("The number of dimensions for should be a multiple of 8 but was [" + dims + "]");
                    }
                }
            });
        private final Parameter<VectorSimilarity> similarity;

        private final Parameter<IndexOptions> indexOptions;

        private final Parameter<Boolean> indexed;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final IndexVersion indexVersionCreated;

        public Builder(String name, IndexVersion indexVersionCreated) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
            final boolean indexedByDefault = indexVersionCreated.onOrAfter(INDEXED_BY_DEFAULT_INDEX_VERSION);
            final boolean defaultInt8Hnsw = indexVersionCreated.onOrAfter(DEFAULT_DENSE_VECTOR_TO_INT8_HNSW);
            this.indexed = Parameter.indexParam(m -> toType(m).fieldType().indexed, indexedByDefault);
            if (indexedByDefault) {
                // Only serialize on newer index versions to prevent breaking existing indices when upgrading
                this.indexed.alwaysSerialize();
            }
            this.similarity = Parameter.enumParam(
                "similarity",
                false,
                m -> toType(m).fieldType().similarity,
                (Supplier<VectorSimilarity>) () -> {
                    if (indexedByDefault && indexed.getValue()) {
                        return elementType.getValue() == ElementType.BIT ? VectorSimilarity.L2_NORM : VectorSimilarity.COSINE;
                    }
                    return null;
                },
                VectorSimilarity.class
            ).acceptsNull().setSerializerCheck((id, ic, v) -> v != null).addValidator(vectorSim -> {
                if (vectorSim == null) {
                    return;
                }
                if (elementType.getValue() == ElementType.BIT && vectorSim != VectorSimilarity.L2_NORM) {
                    throw new IllegalArgumentException(
                        "The [" + VectorSimilarity.L2_NORM + "] similarity is the only supported similarity for bit vectors"
                    );
                }
            });
            this.indexOptions = new Parameter<>(
                "index_options",
                true,
                () -> defaultInt8Hnsw && elementType.getValue() == ElementType.FLOAT && this.indexed.getValue()
                    ? new Int8HnswIndexOptions(
                        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                        null
                    )
                    : null,
                (n, c, o) -> o == null ? null : parseIndexOptions(n, o),
                m -> toType(m).indexOptions,
                (b, n, v) -> {
                    if (v != null) {
                        b.field(n, v);
                    }
                },
                Objects::toString
            ).setSerializerCheck((id, ic, v) -> v != null).addValidator(v -> {
                if (v != null && dims.isConfigured() && dims.get() != null) {
                    v.validateDimension(dims.get());
                }
                if (v != null) {
                    v.validateElementType(elementType.getValue());
                }
            })
                .acceptsNull()
                .setMergeValidator(
                    (previous, current, c) -> previous == null
                        || current == null
                        || Objects.equals(previous, current)
                        || previous.updatableTo(current)
                );
            if (defaultInt8Hnsw) {
                this.indexOptions.alwaysSerialize();
            }
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
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { elementType, dims, indexed, similarity, indexOptions, meta };
        }

        public Builder similarity(VectorSimilarity vectorSimilarity) {
            similarity.setValue(vectorSimilarity);
            return this;
        }

        public Builder dimensions(int dimensions) {
            this.dims.setValue(dimensions);
            return this;
        }

        public Builder elementType(ElementType elementType) {
            this.elementType.setValue(elementType);
            return this;
        }

        @Override
        public DenseVectorFieldMapper build(MapperBuilderContext context) {
            // Validate again here because the dimensions or element type could have been set programmatically,
            // which affects index option validity
            validate();
            return new DenseVectorFieldMapper(
                leafName(),
                new DenseVectorFieldType(
                    context.buildFullName(leafName()),
                    indexVersionCreated,
                    elementType.getValue(),
                    dims.getValue(),
                    indexed.getValue(),
                    similarity.getValue(),
                    indexOptions.getValue(),
                    meta.getValue()
                ),
                builderParams(this, context),
                indexOptions.getValue(),
                indexVersionCreated
            );
        }
    }

    public enum ElementType {

        BYTE {

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

            private KnnByteVectorField createKnnVectorField(String name, byte[] vector, VectorSimilarityFunction function) {
                if (vector == null) {
                    throw new IllegalArgumentException("vector value must not be null");
                }
                FieldType denseVectorFieldType = new FieldType();
                denseVectorFieldType.setVectorAttributes(vector.length, VectorEncoding.BYTE, function);
                denseVectorFieldType.freeze();
                return new KnnByteVectorField(name, vector, denseVectorFieldType);
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
            public double computeSquaredMagnitude(VectorData vectorData) {
                return VectorUtil.dotProduct(vectorData.asByteVector(), vectorData.asByteVector());
            }

            private VectorData parseVectorArray(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                int index = 0;
                byte[] vector = new byte[fieldMapper.fieldType().dims];
                float squaredMagnitude = 0;
                for (XContentParser.Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser()
                    .nextToken()) {
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
                return VectorData.fromBytes(vector);
            }

            private VectorData parseHexEncodedVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                byte[] decodedVector = HexFormat.of().parseHex(context.parser().text());
                fieldMapper.checkDimensionMatches(decodedVector.length, context);
                VectorData vectorData = VectorData.fromBytes(decodedVector);
                double squaredMagnitude = computeSquaredMagnitude(vectorData);
                checkVectorMagnitude(
                    fieldMapper.fieldType().similarity,
                    errorByteElementsAppender(decodedVector),
                    (float) squaredMagnitude
                );
                return vectorData;
            }

            @Override
            VectorData parseKnnVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                XContentParser.Token token = context.parser().currentToken();
                return switch (token) {
                    case START_ARRAY -> parseVectorArray(context, fieldMapper);
                    case VALUE_STRING -> parseHexEncodedVector(context, fieldMapper);
                    default -> throw new ParsingException(
                        context.parser().getTokenLocation(),
                        format("Unsupported type [%s] for provided value [%s]", token, context.parser().text())
                    );
                };
            }

            @Override
            public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                VectorData vectorData = parseKnnVector(context, fieldMapper);
                Field field = createKnnVectorField(
                    fieldMapper.fieldType().name(),
                    vectorData.asByteVector(),
                    fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, this)
                );
                context.doc().addWithKey(fieldMapper.fieldType().name(), field);
            }

            @Override
            int getNumBytes(int dimensions) {
                return dimensions;
            }

            @Override
            ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
                return ByteBuffer.wrap(new byte[numBytes]);
            }

            @Override
            int parseDimensionCount(DocumentParserContext context) throws IOException {
                XContentParser.Token currentToken = context.parser().currentToken();
                return switch (currentToken) {
                    case START_ARRAY -> {
                        int index = 0;
                        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                            index++;
                        }
                        yield index;
                    }
                    case VALUE_STRING -> {
                        byte[] decodedVector = HexFormat.of().parseHex(context.parser().text());
                        yield decodedVector.length;
                    }
                    default -> throw new ParsingException(
                        context.parser().getTokenLocation(),
                        format("Unsupported type [%s] for provided value [%s]", currentToken, context.parser().text())
                    );
                };
            }
        },

        FLOAT {

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

            private KnnFloatVectorField createKnnVectorField(String name, float[] vector, VectorSimilarityFunction function) {
                if (vector == null) {
                    throw new IllegalArgumentException("vector value must not be null");
                }
                FieldType denseVectorFieldType = new FieldType();
                denseVectorFieldType.setVectorAttributes(vector.length, VectorEncoding.FLOAT32, function);
                denseVectorFieldType.freeze();
                return new KnnFloatVectorField(name, vector, denseVectorFieldType);
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
            public double computeSquaredMagnitude(VectorData vectorData) {
                return VectorUtil.dotProduct(vectorData.asFloatVector(), vectorData.asFloatVector());
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
            VectorData parseKnnVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                int index = 0;
                float squaredMagnitude = 0;
                float[] vector = new float[fieldMapper.fieldType().dims];
                for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                    fieldMapper.checkDimensionExceeded(index, context);
                    ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                    float value = context.parser().floatValue(true);
                    vector[index] = value;
                    squaredMagnitude += value * value;
                    index++;
                }
                fieldMapper.checkDimensionMatches(index, context);
                checkVectorBounds(vector);
                checkVectorMagnitude(fieldMapper.fieldType().similarity, errorFloatElementsAppender(vector), squaredMagnitude);
                return VectorData.fromFloats(vector);
            }

            @Override
            int getNumBytes(int dimensions) {
                return dimensions * Float.BYTES;
            }

            @Override
            ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
                return indexVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)
                    ? ByteBuffer.wrap(new byte[numBytes]).order(ByteOrder.LITTLE_ENDIAN)
                    : ByteBuffer.wrap(new byte[numBytes]);
            }
        },

        BIT {

            @Override
            public String toString() {
                return "bit";
            }

            @Override
            public void writeValue(ByteBuffer byteBuffer, float value) {
                byteBuffer.put((byte) value);
            }

            @Override
            public void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException {
                b.value(byteBuffer.get());
            }

            private KnnByteVectorField createKnnVectorField(String name, byte[] vector, VectorSimilarityFunction function) {
                if (vector == null) {
                    throw new IllegalArgumentException("vector value must not be null");
                }
                FieldType denseVectorFieldType = new FieldType();
                denseVectorFieldType.setVectorAttributes(vector.length, VectorEncoding.BYTE, function);
                denseVectorFieldType.freeze();
                return new KnnByteVectorField(name, vector, denseVectorFieldType);
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
            ) {}

            @Override
            public double computeSquaredMagnitude(VectorData vectorData) {
                int count = 0;
                int i = 0;
                byte[] byteBits = vectorData.asByteVector();
                for (int upperBound = byteBits.length & -8; i < upperBound; i += 8) {
                    count += Long.bitCount((long) BitUtil.VH_NATIVE_LONG.get(byteBits, i));
                }

                while (i < byteBits.length) {
                    count += Integer.bitCount(byteBits[i] & 255);
                    ++i;
                }
                return count;
            }

            private VectorData parseVectorArray(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                int index = 0;
                byte[] vector = new byte[fieldMapper.fieldType().dims / Byte.SIZE];
                for (XContentParser.Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser()
                    .nextToken()) {
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
                    if (index >= vector.length) {
                        throw new IllegalArgumentException(
                            "The number of dimensions for field ["
                                + fieldMapper.fieldType().name()
                                + "] should be ["
                                + fieldMapper.fieldType().dims
                                + "] but found ["
                                + (index + 1) * Byte.SIZE
                                + "]"
                        );
                    }
                    vector[index++] = (byte) value;
                }
                fieldMapper.checkDimensionMatches(index * Byte.SIZE, context);
                return VectorData.fromBytes(vector);
            }

            private VectorData parseHexEncodedVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                byte[] decodedVector = HexFormat.of().parseHex(context.parser().text());
                fieldMapper.checkDimensionMatches(decodedVector.length * Byte.SIZE, context);
                return VectorData.fromBytes(decodedVector);
            }

            @Override
            VectorData parseKnnVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                XContentParser.Token token = context.parser().currentToken();
                return switch (token) {
                    case START_ARRAY -> parseVectorArray(context, fieldMapper);
                    case VALUE_STRING -> parseHexEncodedVector(context, fieldMapper);
                    default -> throw new ParsingException(
                        context.parser().getTokenLocation(),
                        format("Unsupported type [%s] for provided value [%s]", token, context.parser().text())
                    );
                };
            }

            @Override
            public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
                VectorData vectorData = parseKnnVector(context, fieldMapper);
                Field field = createKnnVectorField(
                    fieldMapper.fieldType().name(),
                    vectorData.asByteVector(),
                    fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, this)
                );
                context.doc().addWithKey(fieldMapper.fieldType().name(), field);
            }

            @Override
            int getNumBytes(int dimensions) {
                assert dimensions % Byte.SIZE == 0;
                return dimensions / Byte.SIZE;
            }

            @Override
            ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
                return ByteBuffer.wrap(new byte[numBytes]);
            }

            @Override
            int parseDimensionCount(DocumentParserContext context) throws IOException {
                XContentParser.Token currentToken = context.parser().currentToken();
                return switch (currentToken) {
                    case START_ARRAY -> {
                        int index = 0;
                        for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                            index++;
                        }
                        yield index * Byte.SIZE;
                    }
                    case VALUE_STRING -> {
                        byte[] decodedVector = HexFormat.of().parseHex(context.parser().text());
                        yield decodedVector.length * Byte.SIZE;
                    }
                    default -> throw new ParsingException(
                        context.parser().getTokenLocation(),
                        format("Unsupported type [%s] for provided value [%s]", currentToken, context.parser().text())
                    );
                };
            }

            @Override
            public void checkDimensions(Integer dvDims, int qvDims) {
                if (dvDims != null && dvDims != qvDims * Byte.SIZE) {
                    throw new IllegalArgumentException(
                        "The query vector has a different number of dimensions ["
                            + qvDims * Byte.SIZE
                            + "] than the document vectors ["
                            + dvDims
                            + "]."
                    );
                }
            }
        };

        public abstract void writeValue(ByteBuffer byteBuffer, float value);

        public abstract void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException;

        abstract IndexFieldData.Builder fielddataBuilder(DenseVectorFieldType denseVectorFieldType, FieldDataContext fieldDataContext);

        abstract void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException;

        abstract VectorData parseKnnVector(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException;

        abstract int getNumBytes(int dimensions);

        abstract ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes);

        public abstract void checkVectorBounds(float[] vector);

        abstract void checkVectorMagnitude(
            VectorSimilarity similarity,
            Function<StringBuilder, StringBuilder> errorElementsAppender,
            float squaredMagnitude
        );

        public void checkDimensions(Integer dvDims, int qvDims) {
            if (dvDims != null && dvDims != qvDims) {
                throw new IllegalArgumentException(
                    "The query vector has a different number of dimensions [" + qvDims + "] than the document vectors [" + dvDims + "]."
                );
            }
        }

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

        public abstract double computeSquaredMagnitude(VectorData vectorData);

        public static ElementType fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
    }

    static final Map<String, ElementType> namesToElementType = Map.of(
        ElementType.BYTE.toString(),
        ElementType.BYTE,
        ElementType.FLOAT.toString(),
        ElementType.FLOAT,
        ElementType.BIT.toString(),
        ElementType.BIT
    );

    public enum VectorSimilarity {
        L2_NORM {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE, FLOAT -> 1f / (1f + similarity * similarity);
                    case BIT -> (dim - similarity) / dim;
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
                assert elementType != ElementType.BIT;
                return switch (elementType) {
                    case BYTE, FLOAT -> (1 + similarity) / 2f;
                    default -> throw new IllegalArgumentException("Unsupported element type [" + elementType + "]");
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
                    default -> throw new IllegalArgumentException("Unsupported element type [" + elementType + "]");
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
                    default -> throw new IllegalArgumentException("Unsupported element type [" + elementType + "]");
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

    abstract static class IndexOptions implements ToXContent {
        final VectorIndexType type;

        IndexOptions(VectorIndexType type) {
            this.type = type;
        }

        abstract KnnVectorsFormat getVectorsFormat(ElementType elementType);

        final void validateElementType(ElementType elementType) {
            if (type.supportsElementType(elementType) == false) {
                throw new IllegalArgumentException(
                    "[element_type] cannot be [" + elementType.toString() + "] when using index type [" + type + "]"
                );
            }
        }

        abstract boolean updatableTo(IndexOptions update);

        public void validateDimension(int dim) {
            if (type.supportsDimension(dim)) {
                return;
            }
            throw new IllegalArgumentException(type.name + " only supports even dimensions; provided=" + dim);
        }

        abstract boolean doEquals(IndexOptions other);

        abstract int doHashCode();

        @Override
        public final boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null || other.getClass() != getClass()) {
                return false;
            }
            IndexOptions otherOptions = (IndexOptions) other;
            return Objects.equals(type, otherOptions.type) && doEquals(otherOptions);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(type, doHashCode());
        }
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

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return true;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
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

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        },
        INT4_HNSW("int4_hnsw") {
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
                return new Int4HnswIndexOptions(m, efConstruction, confidenceInterval);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims % 2 == 0;
            }
        },
        FLAT("flat") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new FlatIndexOptions();
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return true;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        },
        INT8_FLAT("int8_flat") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int8FlatIndexOptions(confidenceInterval);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        },
        INT4_FLAT("int4_flat") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int4FlatIndexOptions(confidenceInterval);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims % 2 == 0;
            }
        },
        BBQ_HNSW("bbq_hnsw") {
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
                return new BBQHnswIndexOptions(m, efConstruction);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims >= BBQ_MIN_DIMS;
            }
        },
        BBQ_FLAT("bbq_flat") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap) {
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new BBQFlatIndexOptions();
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims >= BBQ_MIN_DIMS;
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

        public abstract boolean supportsElementType(ElementType elementType);

        public abstract boolean supportsDimension(int dims);

        @Override
        public String toString() {
            return name;
        }
    }

    static class Int8FlatIndexOptions extends IndexOptions {
        private final Float confidenceInterval;

        Int8FlatIndexOptions(Float confidenceInterval) {
            super(VectorIndexType.INT8_FLAT);
            this.confidenceInterval = confidenceInterval;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            if (confidenceInterval != null) {
                builder.field("confidence_interval", confidenceInterval);
            }
            builder.endObject();
            return builder;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES813Int8FlatVectorFormat(confidenceInterval, 7, false);
        }

        @Override
        boolean doEquals(IndexOptions o) {
            Int8FlatIndexOptions that = (Int8FlatIndexOptions) o;
            return Objects.equals(confidenceInterval, that.confidenceInterval);
        }

        @Override
        int doHashCode() {
            return Objects.hash(confidenceInterval);
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            return update.type.equals(this.type)
                || update.type.equals(VectorIndexType.HNSW)
                || update.type.equals(VectorIndexType.INT8_HNSW)
                || update.type.equals(VectorIndexType.INT4_HNSW)
                || update.type.equals(VectorIndexType.INT4_FLAT);
        }
    }

    static class FlatIndexOptions extends IndexOptions {

        FlatIndexOptions() {
            super(VectorIndexType.FLAT);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.endObject();
            return builder;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            if (elementType.equals(ElementType.BIT)) {
                return new ES815BitFlatVectorFormat();
            }
            return new ES813FlatVectorFormat();
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            return true;
        }

        @Override
        public boolean doEquals(IndexOptions o) {
            return o instanceof FlatIndexOptions;
        }

        @Override
        public int doHashCode() {
            return Objects.hash(type);
        }
    }

    static class Int4HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;
        private final float confidenceInterval;

        Int4HnswIndexOptions(int m, int efConstruction, Float confidenceInterval) {
            super(VectorIndexType.INT4_HNSW);
            this.m = m;
            this.efConstruction = efConstruction;
            // The default confidence interval for int4 is dynamic quantiles, this provides the best relevancy and is
            // effectively required for int4 to behave well across a wide range of data.
            this.confidenceInterval = confidenceInterval == null ? 0f : confidenceInterval;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES814HnswScalarQuantizedVectorsFormat(m, efConstruction, confidenceInterval, 4, true);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            builder.field("confidence_interval", confidenceInterval);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean doEquals(IndexOptions o) {
            Int4HnswIndexOptions that = (Int4HnswIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction && Objects.equals(confidenceInterval, that.confidenceInterval);
        }

        @Override
        public int doHashCode() {
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

        @Override
        boolean updatableTo(IndexOptions update) {
            boolean updatable = update.type.equals(this.type);
            if (updatable) {
                Int4HnswIndexOptions int4HnswIndexOptions = (Int4HnswIndexOptions) update;
                // fewer connections would break assumptions on max number of connections (based on largest previous graph) during merge
                // quantization could not behave as expected with different confidence intervals (and quantiles) to be created
                updatable = int4HnswIndexOptions.m >= this.m && confidenceInterval == int4HnswIndexOptions.confidenceInterval;
            }
            return updatable;
        }
    }

    static class Int4FlatIndexOptions extends IndexOptions {
        private final float confidenceInterval;

        Int4FlatIndexOptions(Float confidenceInterval) {
            super(VectorIndexType.INT4_FLAT);
            // The default confidence interval for int4 is dynamic quantiles, this provides the best relevancy and is
            // effectively required for int4 to behave well across a wide range of data.
            this.confidenceInterval = confidenceInterval == null ? 0f : confidenceInterval;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES813Int8FlatVectorFormat(confidenceInterval, 4, true);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("confidence_interval", confidenceInterval);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean doEquals(IndexOptions o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Int4FlatIndexOptions that = (Int4FlatIndexOptions) o;
            return Objects.equals(confidenceInterval, that.confidenceInterval);
        }

        @Override
        public int doHashCode() {
            return Objects.hash(confidenceInterval);
        }

        @Override
        public String toString() {
            return "{type=" + type + ", confidence_interval=" + confidenceInterval + "}";
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            // TODO: add support for updating from flat, hnsw, and int8_hnsw and updating params
            return update.type.equals(this.type)
                || update.type.equals(VectorIndexType.HNSW)
                || update.type.equals(VectorIndexType.INT8_HNSW)
                || update.type.equals(VectorIndexType.INT4_HNSW);
        }

    }

    static class Int8HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;
        private final Float confidenceInterval;

        Int8HnswIndexOptions(int m, int efConstruction, Float confidenceInterval) {
            super(VectorIndexType.INT8_HNSW);
            this.m = m;
            this.efConstruction = efConstruction;
            this.confidenceInterval = confidenceInterval;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES814HnswScalarQuantizedVectorsFormat(m, efConstruction, confidenceInterval, 7, false);
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
        public boolean doEquals(IndexOptions o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Int8HnswIndexOptions that = (Int8HnswIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction && Objects.equals(confidenceInterval, that.confidenceInterval);
        }

        @Override
        public int doHashCode() {
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

        @Override
        boolean updatableTo(IndexOptions update) {
            boolean updatable;
            if (update.type.equals(this.type)) {
                Int8HnswIndexOptions int8HnswIndexOptions = (Int8HnswIndexOptions) update;
                // fewer connections would break assumptions on max number of connections (based on largest previous graph) during merge
                // quantization could not behave as expected with different confidence intervals (and quantiles) to be created
                updatable = int8HnswIndexOptions.m >= this.m;
                updatable &= confidenceInterval == null
                    || int8HnswIndexOptions.confidenceInterval != null
                        && confidenceInterval.equals(int8HnswIndexOptions.confidenceInterval);
            } else {
                updatable = update.type.equals(VectorIndexType.INT4_HNSW) && ((Int4HnswIndexOptions) update).m >= this.m;
            }
            return updatable;
        }
    }

    static class HnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;

        HnswIndexOptions(int m, int efConstruction) {
            super(VectorIndexType.HNSW);
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            if (elementType == ElementType.BIT) {
                return new ES815HnswBitVectorsFormat(m, efConstruction);
            }
            return new Lucene99HnswVectorsFormat(m, efConstruction, 1, null);
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            boolean updatable = update.type.equals(this.type);
            if (updatable) {
                // fewer connections would break assumptions on max number of connections (based on largest previous graph) during merge
                HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) update;
                updatable = hnswIndexOptions.m >= this.m;
            }
            return updatable
                || (update.type.equals(VectorIndexType.INT8_HNSW) && ((Int8HnswIndexOptions) update).m >= m)
                || (update.type.equals(VectorIndexType.INT4_HNSW) && ((Int4HnswIndexOptions) update).m >= m);
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
        public boolean doEquals(IndexOptions o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HnswIndexOptions that = (HnswIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction;
        }

        @Override
        public int doHashCode() {
            return Objects.hash(m, efConstruction);
        }

        @Override
        public String toString() {
            return "{type=" + type + ", m=" + m + ", ef_construction=" + efConstruction + "}";
        }
    }

    static class BBQHnswIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;

        BBQHnswIndexOptions(int m, int efConstruction) {
            super(VectorIndexType.BBQ_HNSW);
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES816HnswBinaryQuantizedVectorsFormat(m, efConstruction);
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            return update.type.equals(this.type);
        }

        @Override
        boolean doEquals(IndexOptions other) {
            BBQHnswIndexOptions that = (BBQHnswIndexOptions) other;
            return m == that.m && efConstruction == that.efConstruction;
        }

        @Override
        int doHashCode() {
            return Objects.hash(m, efConstruction);
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
        public void validateDimension(int dim) {
            if (type.supportsDimension(dim)) {
                return;
            }
            throw new IllegalArgumentException(type.name + " does not support dimensions fewer than " + BBQ_MIN_DIMS + "; provided=" + dim);
        }
    }

    static class BBQFlatIndexOptions extends IndexOptions {
        private final int CLASS_NAME_HASH = this.getClass().getName().hashCode();

        BBQFlatIndexOptions() {
            super(VectorIndexType.BBQ_FLAT);
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType) {
            assert elementType == ElementType.FLOAT;
            return new ES816BinaryQuantizedVectorsFormat();
        }

        @Override
        boolean updatableTo(IndexOptions update) {
            return update.type.equals(this.type);
        }

        @Override
        boolean doEquals(IndexOptions other) {
            return other instanceof BBQFlatIndexOptions;
        }

        @Override
        int doHashCode() {
            return CLASS_NAME_HASH;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.endObject();
            return builder;
        }

        @Override
        public void validateDimension(int dim) {
            if (type.supportsDimension(dim)) {
                return;
            }
            throw new IllegalArgumentException(type.name + " does not support dimensions fewer than " + BBQ_MIN_DIMS + "; provided=" + dim);
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
        private final IndexOptions indexOptions;

        public DenseVectorFieldType(
            String name,
            IndexVersion indexVersionCreated,
            ElementType elementType,
            Integer dims,
            boolean indexed,
            VectorSimilarity similarity,
            IndexOptions indexOptions,
            Map<String, String> meta
        ) {
            super(name, indexed, false, indexed == false, TextSearchInfo.NONE, meta);
            this.elementType = elementType;
            this.dims = dims;
            this.indexed = indexed;
            this.similarity = similarity;
            this.indexVersionCreated = indexVersionCreated;
            this.indexOptions = indexOptions;
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

        public Query createExactKnnQuery(VectorData queryVector, Float vectorSimilarity) {
            if (isIndexed() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }
            Query knnQuery = switch (elementType) {
                case BYTE -> createExactKnnByteQuery(queryVector.asByteVector());
                case FLOAT -> createExactKnnFloatQuery(queryVector.asFloatVector());
                case BIT -> createExactKnnBitQuery(queryVector.asByteVector());
            };
            if (vectorSimilarity != null) {
                knnQuery = new VectorSimilarityQuery(knnQuery, vectorSimilarity, similarity.score(vectorSimilarity, elementType, dims));
            }
            return knnQuery;
        }

        private Query createExactKnnBitQuery(byte[] queryVector) {
            elementType.checkDimensions(dims, queryVector.length);
            return new DenseVectorQuery.Bytes(queryVector, name());
        }

        private Query createExactKnnByteQuery(byte[] queryVector) {
            elementType.checkDimensions(dims, queryVector.length);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorByteElementsAppender(queryVector), squaredMagnitude);
            }
            return new DenseVectorQuery.Bytes(queryVector, name());
        }

        private Query createExactKnnFloatQuery(float[] queryVector) {
            elementType.checkDimensions(dims, queryVector.length);
            elementType.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorFloatElementsAppender(queryVector), squaredMagnitude);
                if (similarity == VectorSimilarity.COSINE
                    && indexVersionCreated.onOrAfter(NORMALIZE_COSINE)
                    && isNotUnitVector(squaredMagnitude)) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    queryVector = Arrays.copyOf(queryVector, queryVector.length);
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] /= length;
                    }
                }
            }
            return new DenseVectorQuery.Floats(queryVector, name());
        }

        public Query createKnnQuery(
            VectorData queryVector,
            Integer k,
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
            return switch (getElementType()) {
                case BYTE -> createKnnByteQuery(queryVector.asByteVector(), k, numCands, filter, similarityThreshold, parentFilter);
                case FLOAT -> createKnnFloatQuery(queryVector.asFloatVector(), k, numCands, filter, similarityThreshold, parentFilter);
                case BIT -> createKnnBitQuery(queryVector.asByteVector(), k, numCands, filter, similarityThreshold, parentFilter);
            };
        }

        private Query createKnnBitQuery(
            byte[] queryVector,
            Integer k,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter
        ) {
            elementType.checkDimensions(dims, queryVector.length);
            Query knnQuery = parentFilter != null
                ? new ESDiversifyingChildrenByteKnnVectorQuery(name(), queryVector, filter, k, numCands, parentFilter)
                : new ESKnnByteVectorQuery(name(), queryVector, k, numCands, filter);
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, elementType, dims)
                );
            }
            return knnQuery;
        }

        private Query createKnnByteQuery(
            byte[] queryVector,
            Integer k,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter
        ) {
            elementType.checkDimensions(dims, queryVector.length);

            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorByteElementsAppender(queryVector), squaredMagnitude);
            }
            Query knnQuery = parentFilter != null
                ? new ESDiversifyingChildrenByteKnnVectorQuery(name(), queryVector, filter, k, numCands, parentFilter)
                : new ESKnnByteVectorQuery(name(), queryVector, k, numCands, filter);
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, elementType, dims)
                );
            }
            return knnQuery;
        }

        private Query createKnnFloatQuery(
            float[] queryVector,
            Integer k,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter
        ) {
            elementType.checkDimensions(dims, queryVector.length);
            elementType.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = VectorUtil.dotProduct(queryVector, queryVector);
                elementType.checkVectorMagnitude(similarity, ElementType.errorFloatElementsAppender(queryVector), squaredMagnitude);
                if (similarity == VectorSimilarity.COSINE
                    && indexVersionCreated.onOrAfter(NORMALIZE_COSINE)
                    && isNotUnitVector(squaredMagnitude)) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    queryVector = Arrays.copyOf(queryVector, queryVector.length);
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] /= length;
                    }
                }
            }
            Query knnQuery = parentFilter != null
                ? new ESDiversifyingChildrenFloatKnnVectorQuery(name(), queryVector, filter, k, numCands, parentFilter)
                : new ESKnnFloatVectorQuery(name(), queryVector, k, numCands, filter);
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
        BuilderParams params,
        IndexOptions indexOptions,
        IndexVersion indexCreatedVersion
    ) {
        super(simpleName, mappedFieldType, params);
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
                    + fullPath()
                    + "] of type ["
                    + typeName()
                    + "] doesn't support indexing multiple values for the same field in the same document"
            );
        }
        if (Token.VALUE_NULL == context.parser().currentToken()) {
            return;
        }
        if (fieldType().dims == null) {
            int dims = fieldType().elementType.parseDimensionCount(context);
            if (fieldType().indexOptions != null) {
                fieldType().indexOptions.validateDimension(dims);
            }
            DenseVectorFieldType updatedDenseVectorFieldType = new DenseVectorFieldType(
                fieldType().name(),
                indexCreatedVersion,
                fieldType().elementType,
                dims,
                fieldType().indexed,
                fieldType().similarity,
                fieldType().indexOptions,
                fieldType().meta()
            );
            Mapper update = new DenseVectorFieldMapper(
                leafName(),
                updatedDenseVectorFieldType,
                builderParams,
                indexOptions,
                indexCreatedVersion
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
            ? elementType.getNumBytes(dims) + MAGNITUDE_BYTES
            : elementType.getNumBytes(dims);

        ByteBuffer byteBuffer = elementType.createByteBuffer(indexCreatedVersion, numBytes);
        VectorData vectorData = elementType.parseKnnVector(context, this);
        vectorData.addToBuffer(byteBuffer);
        if (indexCreatedVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)) {
            // encode vector magnitude at the end
            double dotProduct = elementType.computeSquaredMagnitude(vectorData);
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
                    + fullPath()
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
                    + fullPath()
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
        return new Builder(leafName(), indexCreatedVersion).init(this);
    }

    private static IndexOptions parseIndexOptions(String fieldName, Object propNode) {
        @SuppressWarnings("unchecked")
        Map<String, ?> indexOptionsMap = (Map<String, ?>) propNode;
        Object typeNode = indexOptionsMap.remove("type");
        if (typeNode == null) {
            throw new MapperParsingException("[index_options] requires field [type] to be configured");
        }
        String type = XContentMapValues.nodeStringValue(typeNode);
        Optional<VectorIndexType> vectorIndexType = VectorIndexType.fromString(type);
        if (vectorIndexType.isEmpty()) {
            throw new MapperParsingException("Unknown vector index options type [" + type + "] for field [" + fieldName + "]");
        }
        VectorIndexType parsedType = vectorIndexType.get();
        return parsedType.parseIndexOptions(fieldName, indexOptionsMap);
    }

    /**
     * @return the custom kNN vectors format that is configured for this field or
     * {@code null} if the default format should be used.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(KnnVectorsFormat defaultFormat) {
        final KnnVectorsFormat format;
        if (indexOptions == null) {
            format = fieldType().elementType == ElementType.BIT ? new ES815HnswBitVectorsFormat() : defaultFormat;
        } else {
            format = indexOptions.getVectorsFormat(fieldType().elementType);
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
    protected SyntheticSourceSupport syntheticSourceSupport() {
        var loader = fieldType().indexed
            ? new IndexedSyntheticFieldLoader(indexCreatedVersion, fieldType().similarity)
            : new DocValuesSyntheticFieldLoader(indexCreatedVersion);

        return new SyntheticSourceSupport.Native(loader);
    }

    private class IndexedSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private FloatVectorValues values;
        private ByteVectorValues byteVectorValues;
        private boolean hasValue;
        private boolean hasMagnitude;
        private int ord;

        private final IndexVersion indexCreatedVersion;
        private final VectorSimilarity vectorSimilarity;
        private NumericDocValues magnitudeReader;

        private IndexedSyntheticFieldLoader(IndexVersion indexCreatedVersion, VectorSimilarity vectorSimilarity) {
            this.indexCreatedVersion = indexCreatedVersion;
            this.vectorSimilarity = vectorSimilarity;
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            values = leafReader.getFloatVectorValues(fullPath());
            if (values != null) {
                if (indexCreatedVersion.onOrAfter(NORMALIZE_COSINE) && VectorSimilarity.COSINE.equals(vectorSimilarity)) {
                    magnitudeReader = leafReader.getNumericDocValues(fullPath() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                }
                KnnVectorValues.DocIndexIterator iterator = values.iterator();
                return docId -> {
                    hasValue = docId == iterator.advance(docId);
                    hasMagnitude = hasValue && magnitudeReader != null && magnitudeReader.advanceExact(docId);
                    ord = iterator.index();
                    return hasValue;
                };
            }
            byteVectorValues = leafReader.getByteVectorValues(fullPath());
            if (byteVectorValues != null) {
                KnnVectorValues.DocIndexIterator iterator = byteVectorValues.iterator();
                return docId -> {
                    hasValue = docId == iterator.advance(docId);
                    ord = iterator.index();
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
            b.startArray(leafName());
            if (values != null) {
                for (float v : values.vectorValue(ord)) {
                    if (hasMagnitude) {
                        b.value(v * magnitude);
                    } else {
                        b.value(v);
                    }
                }
            } else if (byteVectorValues != null) {
                byte[] vectorValue = byteVectorValues.vectorValue(ord);
                for (byte value : vectorValue) {
                    b.value(value);
                }
            }
            b.endArray();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }

    private class DocValuesSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private BinaryDocValues values;
        private boolean hasValue;
        private final IndexVersion indexCreatedVersion;

        private DocValuesSyntheticFieldLoader(IndexVersion indexCreatedVersion) {
            this.indexCreatedVersion = indexCreatedVersion;
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            values = leafReader.getBinaryDocValues(fullPath());
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
            b.startArray(leafName());
            BytesRef ref = values.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
            if (indexCreatedVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)) {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            int dims = fieldType().elementType == ElementType.BIT ? fieldType().dims / Byte.SIZE : fieldType().dims;
            for (int dim = 0; dim < dims; dim++) {
                fieldType().elementType.readAndWriteValue(byteBuffer, b);
            }
            b.endArray();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }
}
