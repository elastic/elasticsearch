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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParser;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorBlockLoaderProcessor;
import org.elasticsearch.index.mapper.blockloader.docvalues.DenseVectorFromBinaryBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.DiversifyingChildrenIVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.DiversifyingParentBlockQuery;
import org.elasticsearch.search.vectors.ESDiversifyingChildrenByteKnnVectorQuery;
import org.elasticsearch.search.vectors.ESDiversifyingChildrenFloatKnnVectorQuery;
import org.elasticsearch.search.vectors.ESKnnByteVectorQuery;
import org.elasticsearch.search.vectors.ESKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING;
import static org.elasticsearch.index.IndexVersions.DISK_BBQ_QUANTIZE_BITS;
import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER;

/**
 * A {@link FieldMapper} for indexing a dense vector of floats.
 */
public class DenseVectorFieldMapper extends FieldMapper {
    public static final String COSINE_MAGNITUDE_FIELD_SUFFIX = "._magnitude";
    public static final int BBQ_MIN_DIMS = 64;

    private static final int DEFAULT_BBQ_IVF_QUANTIZE_BITS = 1;

    /**
     * The heuristic to utilize when executing a filtered search against vectors indexed in an HNSW graph.
     */
    public enum FilterHeuristic {
        /**
         * This heuristic searches the entire graph, doing vector comparisons in all immediate neighbors
         * but only collects vectors that match the filtering criteria.
         */
        FANOUT {
            static final KnnSearchStrategy FANOUT_STRATEGY = new KnnSearchStrategy.Hnsw(0);

            @Override
            public KnnSearchStrategy getKnnSearchStrategy() {
                return FANOUT_STRATEGY;
            }
        },
        /**
         * This heuristic will only compare vectors that match the filtering criteria.
         */
        ACORN {
            static final KnnSearchStrategy ACORN_STRATEGY = new KnnSearchStrategy.Hnsw(60);

            @Override
            public KnnSearchStrategy getKnnSearchStrategy() {
                return ACORN_STRATEGY;
            }
        };

        public abstract KnnSearchStrategy getKnnSearchStrategy();
    }

    public static final Setting<FilterHeuristic> HNSW_FILTER_HEURISTIC = Setting.enumSetting(FilterHeuristic.class, s -> {
        IndexVersion version = SETTING_INDEX_VERSION_CREATED.get(s);
        if (version.onOrAfter(IndexVersions.DEFAULT_TO_ACORN_HNSW_FILTER_HEURISTIC)) {
            return FilterHeuristic.ACORN.toString();
        }
        return FilterHeuristic.FANOUT.toString();
    },
        "index.dense_vector.hnsw_filter_heuristic",
        fh -> {},
        Setting.Property.IndexScope,
        Setting.Property.ServerlessPublic,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> HNSW_EARLY_TERMINATION = Setting.boolSetting(
        "index.dense_vector.hnsw_enable_early_termination",
        s -> {
            IndexVersion version = SETTING_INDEX_VERSION_CREATED.get(s);
            return String.valueOf(version.onOrAfter(IndexVersions.DEFAULT_HNSW_EARLY_TERMINATION));
        },
        Setting.Property.IndexScope,
        Setting.Property.ServerlessPublic,
        Setting.Property.Dynamic
    );

    private static boolean hasRescoreIndexVersion(IndexVersion version) {
        return version.onOrAfter(IndexVersions.ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS)
            || version.between(IndexVersions.ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS_BACKPORT_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
    }

    private static boolean allowsZeroRescore(IndexVersion version) {
        return version.onOrAfter(IndexVersions.RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS)
            || version.between(
                IndexVersions.RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS_BACKPORT_8_X,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

    private static boolean defaultOversampleForBBQ(IndexVersion version) {
        return version.onOrAfter(IndexVersions.DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ)
            || version.between(IndexVersions.DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ_BACKPORT_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
    }

    public static final IndexVersion MAGNITUDE_STORED_INDEX_VERSION = IndexVersions.V_7_5_0;
    public static final IndexVersion INDEXED_BY_DEFAULT_INDEX_VERSION = IndexVersions.FIRST_DETACHED_INDEX_VERSION;
    public static final IndexVersion NORMALIZE_COSINE = IndexVersions.NORMALIZED_VECTOR_COSINE;
    public static final IndexVersion DEFAULT_TO_INT8 = IndexVersions.DEFAULT_DENSE_VECTOR_TO_INT8_HNSW;
    public static final IndexVersion DEFAULT_TO_BBQ = IndexVersions.DEFAULT_DENSE_VECTOR_TO_BBQ_HNSW;
    public static final IndexVersion LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION = IndexVersions.V_8_9_0;

    public static final NodeFeature RESCORE_VECTOR_QUANTIZED_VECTOR_MAPPING = new NodeFeature("mapper.dense_vector.rescore_vector");
    public static final NodeFeature RESCORE_ZERO_VECTOR_QUANTIZED_VECTOR_MAPPING = new NodeFeature(
        "mapper.dense_vector.rescore_zero_vector"
    );
    public static final NodeFeature USE_DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ = new NodeFeature(
        "mapper.dense_vector.default_oversample_value_for_bbq"
    );

    public static final String CONTENT_TYPE = "dense_vector";
    public static final short MAX_DIMS_COUNT = 4096; // maximum allowed number of dimensions
    public static final int MAX_DIMS_COUNT_BIT = 4096 * Byte.SIZE; // maximum allowed number of dimensions

    public static final short MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING = 128; // minimum number of dims for floats to be dynamically mapped to
    // vector
    public static final int MAGNITUDE_BYTES = 4;
    public static final int OVERSAMPLE_LIMIT = 10_000; // Max oversample allowed
    public static final float DEFAULT_OVERSAMPLE = 3.0F; // Default oversample value
    public static final int BBQ_DIMS_DEFAULT_THRESHOLD = 384; // Lower bound for dimensions for using bbq_hnsw as default index options

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
        }, m -> toType(m).fieldType().element.elementType(), XContentBuilder::field, Objects::toString);
        private final Parameter<Integer> dims;
        private final Parameter<VectorSimilarity> similarity;

        private final Parameter<DenseVectorIndexOptions> indexOptions;

        private final Parameter<Boolean> indexed;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final IndexVersion indexVersionCreated;
        final boolean isExcludeSourceVectors;
        private final List<VectorsFormatProvider> vectorsFormatProviders;

        public Builder(
            String name,
            IndexVersion indexVersionCreated,
            boolean isExcludeSourceVectors,
            List<VectorsFormatProvider> vectorsFormatProviders
        ) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
            this.vectorsFormatProviders = vectorsFormatProviders;
            // This is defined as updatable because it can be updated once, from [null] to a valid dim size,
            // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
            this.dims = new Parameter<>("dims", true, () -> null, (n, c, o) -> {
                if (o instanceof Integer == false) {
                    throw new MapperParsingException("Property [dims] on field [" + n + "] must be an integer but got [" + o + "]");
                }

                return XContentMapValues.nodeIntegerValue(o);
            }, m -> toType(m).fieldType().dims, XContentBuilder::field, Objects::toString).setSerializerCheck((id, ic, v) -> v != null)
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
                            throw new MapperParsingException(
                                "The number of dimensions for should be a multiple of 8 but was [" + dims + "]"
                            );
                        }
                    }
                });
            this.isExcludeSourceVectors = isExcludeSourceVectors;
            final boolean indexedByDefault = indexVersionCreated.onOrAfter(INDEXED_BY_DEFAULT_INDEX_VERSION);
            final boolean defaultInt8Hnsw = indexVersionCreated.onOrAfter(IndexVersions.DEFAULT_DENSE_VECTOR_TO_INT8_HNSW);
            final boolean defaultBBQHnsw = indexVersionCreated.onOrAfter(IndexVersions.DEFAULT_DENSE_VECTOR_TO_BBQ_HNSW);
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
                () -> defaultIndexOptions(defaultInt8Hnsw, defaultBBQHnsw),
                (n, c, o) -> o == null ? null : parseIndexOptions(n, o, indexVersionCreated),
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
            if (defaultInt8Hnsw || defaultBBQHnsw) {
                if (defaultBBQHnsw == false || (dims != null && dims.isConfigured())) {
                    this.indexOptions.alwaysSerialize();
                }
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

        private DenseVectorIndexOptions defaultIndexOptions(boolean defaultInt8Hnsw, boolean defaultBBQHnsw) {
            if (elementType.getValue() != ElementType.FLOAT || indexed.getValue() == false) {
                return null;
            }

            boolean dimIsConfigured = dims != null && dims.isConfigured();
            if (defaultBBQHnsw && dimIsConfigured == false) {
                // Delay selecting the default index options until dimensions are configured.
                // This applies only to indices that are eligible to use BBQ as the default,
                // since prior to this change, the default was selected eagerly.
                return null;
            }

            if (defaultBBQHnsw && dimIsConfigured && dims.getValue() >= BBQ_DIMS_DEFAULT_THRESHOLD) {
                return new BBQHnswIndexOptions(
                    Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                    Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                    false,
                    new RescoreVector(DEFAULT_OVERSAMPLE)
                );
            } else if (defaultInt8Hnsw) {
                return new Int8HnswIndexOptions(
                    Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                    Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                    null,
                    false,
                    null
                );
            }
            return null;
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

        public Builder indexOptions(DenseVectorIndexOptions indexOptions) {
            this.indexOptions.setValue(indexOptions);
            return this;
        }

        @Override
        public DenseVectorFieldMapper build(MapperBuilderContext context) {
            // Validate again here because the dimensions or element type could have been set programmatically,
            // which affects index option validity
            validate();
            boolean isExcludeSourceVectorsFinal = context.isSourceSynthetic() == false && indexed.getValue() && isExcludeSourceVectors;
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
                    meta.getValue(),
                    context.isSourceSynthetic()
                ),
                builderParams(this, context),
                indexOptions.getValue(),
                indexVersionCreated,
                isExcludeSourceVectorsFinal,
                vectorsFormatProviders
            );
        }
    }

    public enum ElementType {
        BYTE,
        FLOAT,
        BFLOAT16,
        BIT;

        public static ElementType fromString(String name) {
            return valueOf(name.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public static final Element BYTE_ELEMENT = new ByteElement();
    public static final Element FLOAT_ELEMENT = new FloatElement();
    public static final Element BFLOAT16_ELEMENT = new BFloat16Element();
    public static final Element BIT_ELEMENT = new BitElement();

    public static final Map<String, ElementType> namesToElementType = Map.of(
        ElementType.BYTE.toString(),
        ElementType.BYTE,
        ElementType.FLOAT.toString(),
        ElementType.FLOAT,
        ElementType.BFLOAT16.toString(),
        ElementType.BFLOAT16,
        ElementType.BIT.toString(),
        ElementType.BIT
    );

    public abstract static class Element {

        public static Element getElement(ElementType elementType) {
            return switch (elementType) {
                case FLOAT -> FLOAT_ELEMENT;
                case BFLOAT16 -> BFLOAT16_ELEMENT;
                case BYTE -> BYTE_ELEMENT;
                case BIT -> BIT_ELEMENT;
            };
        }

        /**
         * Checks the input {@code vector} is one of the {@code possibleTypes},
         * and returns the first type that it matches
         */
        public static ElementType checkValidVector(float[] vector, ElementType... possibleTypes) {
            assert possibleTypes.length != 0;
            // we're looking for one valid allowed type
            // assume the types are in order of specificity
            StringBuilder[] errors = new StringBuilder[possibleTypes.length];
            for (int i = 0; i < possibleTypes.length; i++) {
                StringBuilder error = getElement(possibleTypes[i]).checkVectorErrors(vector);
                if (error == null) {
                    // this one works - use it
                    return possibleTypes[i];
                } else {
                    errors[i] = error;
                }
            }

            // oh dear, none of the possible types work with this vector. Generate the error message and throw.
            StringBuilder message = new StringBuilder();
            for (int i = 0; i < possibleTypes.length; i++) {
                if (i > 0) {
                    message.append(" ");
                }
                message.append("Vector is not a ").append(possibleTypes[i]).append(" vector: ").append(errors[i]);
            }
            throw new IllegalArgumentException(FloatElement.appendErrorElements(message, vector).toString());
        }

        public abstract ElementType elementType();

        public abstract void writeValues(ByteBuffer byteBuffer, float[] values);

        public abstract void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException;

        abstract IndexFieldData.Builder fielddataBuilder(DenseVectorFieldType denseVectorFieldType, FieldDataContext fieldDataContext);

        abstract void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException;

        public abstract VectorData parseKnnVector(
            DocumentParserContext context,
            int dims,
            IntBooleanConsumer dimChecker,
            VectorSimilarity similarity
        ) throws IOException;

        public abstract int getNumBytes(int dimensions);

        public abstract ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes);

        public boolean isUnitVector(float squaredMagnitude) {
            return Math.abs(squaredMagnitude - 1.0f) < 1e-3f;
        }

        public void checkVectorBounds(float[] vector) {
            StringBuilder errors = checkVectorErrors(vector);
            if (errors != null) {
                throw new IllegalArgumentException(FloatElement.appendErrorElements(errors, vector).toString());
            }
        }

        StringBuilder checkVectorErrors(float[] vector) {
            return checkNanAndInfinite(vector);
        }

        abstract void checkVectorMagnitude(
            VectorSimilarity similarity,
            UnaryOperator<StringBuilder> errorElementsAppender,
            float squaredMagnitude
        );

        public abstract double computeSquaredMagnitude(VectorData vectorData);

        public void checkDimensions(Integer dvDims, int qvDims) {
            if (dvDims != null && dvDims != qvDims) {
                throw new IllegalArgumentException(
                    "The query vector has a different number of dimensions [" + qvDims + "] than the document vectors [" + dvDims + "]."
                );
            }
        }

        public int parseDimensionCount(DocumentParserContext context) throws IOException {
            int index = 0;
            for (Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser().nextToken()) {
                index++;
            }
            return index;
        }

        StringBuilder checkNanAndInfinite(float[] vector) {
            StringBuilder errorBuilder = null;

            for (int index = 0; index < vector.length; ++index) {
                float value = vector[index];

                if (Float.isNaN(value)) {
                    errorBuilder = new StringBuilder(
                        "element_type ["
                            + elementType()
                            + "] vectors do not support NaN values but found ["
                            + value
                            + "] at dim ["
                            + index
                            + "];"
                    );
                    break;
                }

                if (Float.isInfinite(value)) {
                    errorBuilder = new StringBuilder(
                        "element_type ["
                            + elementType()
                            + "] vectors do not support infinite values but found ["
                            + value
                            + "] at dim ["
                            + index
                            + "];"
                    );
                    break;
                }
            }

            return errorBuilder;
        }
    }

    private static class ByteElement extends Element {

        @Override
        public ElementType elementType() {
            return ElementType.BYTE;
        }

        @Override
        public void writeValues(ByteBuffer byteBuffer, float[] values) {
            for (float f : values) {
                byteBuffer.put((byte) f);
            }
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
                elementType(),
                denseVectorFieldType.dims,
                denseVectorFieldType.indexed,
                r -> r
            );
        }

        @Override
        StringBuilder checkVectorErrors(float[] vector) {
            StringBuilder errors = super.checkNanAndInfinite(vector);
            if (errors != null) {
                return errors;
            }

            for (int index = 0; index < vector.length; ++index) {
                float value = vector[index];

                if (value % 1.0f != 0.0f) {
                    errors = new StringBuilder(
                        "element_type ["
                            + elementType()
                            + "] vectors only support non-decimal values but found decimal value ["
                            + value
                            + "] at dim ["
                            + index
                            + "];"
                    );
                    break;
                }

                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    errors = new StringBuilder(
                        "element_type ["
                            + elementType()
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

            return errors;
        }

        @Override
        void checkVectorMagnitude(VectorSimilarity similarity, UnaryOperator<StringBuilder> appender, float squaredMagnitude) {
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
            return ESVectorUtil.dotProduct(vectorData.asByteVector(), vectorData.asByteVector());
        }

        @Override
        public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
            VectorData vectorData = parseKnnVector(context, fieldMapper.fieldType().dims, (i, end) -> {
                if (end) {
                    fieldMapper.checkDimensionMatches(i, context);
                } else {
                    fieldMapper.checkDimensionExceeded(i, context);
                }
            }, fieldMapper.fieldType().similarity);
            Field field = createKnnVectorField(
                fieldMapper.fieldType().name(),
                vectorData.asByteVector(),
                fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, elementType())
            );
            context.doc().addWithKey(fieldMapper.fieldType().name(), field);
        }

        VectorData parseVectorArray(DocumentParserContext context, int dims, IntBooleanConsumer dimChecker, VectorSimilarity similarity)
            throws IOException {
            int index = 0;
            byte[] vector = new byte[dims];
            float squaredMagnitude = 0;
            for (XContentParser.Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser()
                .nextToken()) {
                dimChecker.accept(index, false);
                ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                final int value;
                if (context.parser().numberType() != XContentParser.NumberType.INT) {
                    float floatValue = context.parser().floatValue(true);
                    if (floatValue % 1.0f != 0.0f) {
                        throw new IllegalArgumentException(
                            "element_type ["
                                + elementType()
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
                            + elementType()
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
            dimChecker.accept(index, true);
            checkVectorMagnitude(similarity, errorElementsAppender(vector), squaredMagnitude);
            return VectorData.fromBytes(vector);
        }

        VectorData parseStringValue(
            String s,
            IntBooleanConsumer dimChecker,
            VectorSimilarity similarity,
            Function<String, byte[]> decoder
        ) {
            byte[] decodedVector = decoder.apply(s);
            dimChecker.accept(decodedVector.length, true);
            VectorData vectorData = VectorData.fromBytes(decodedVector);
            double squaredMagnitude = computeSquaredMagnitude(vectorData);
            checkVectorMagnitude(similarity, errorElementsAppender(decodedVector), (float) squaredMagnitude);
            return vectorData;
        }

        VectorData parseHexEncodedVector(String s, IntBooleanConsumer dimChecker, VectorSimilarity similarity) {
            return parseStringValue(s, dimChecker, similarity, HexFormat.of()::parseHex);
        }

        VectorData parseBase64EncodedVector(String s, IntBooleanConsumer dimChecker, VectorSimilarity similarity) {
            return parseStringValue(s, dimChecker, similarity, Base64.getDecoder()::decode);
        }

        @Override
        public VectorData parseKnnVector(
            DocumentParserContext context,
            int dims,
            IntBooleanConsumer dimChecker,
            VectorSimilarity similarity
        ) throws IOException {
            XContentParser.Token token = context.parser().currentToken();
            return switch (token) {
                case START_ARRAY -> parseVectorArray(context, dims, dimChecker, similarity);
                case VALUE_STRING -> {
                    String s = context.parser().text();
                    if (s.length() == dims * 2) {
                        try {
                            yield parseHexEncodedVector(s, dimChecker, similarity);
                        } catch (IllegalArgumentException e) {
                            yield parseBase64EncodedVector(s, dimChecker, similarity);
                        }
                    } else {
                        try {
                            yield parseBase64EncodedVector(s, dimChecker, similarity);
                        } catch (IllegalArgumentException e) {
                            yield parseHexEncodedVector(s, dimChecker, similarity);
                        }
                    }
                }
                default -> throw new ParsingException(
                    context.parser().getTokenLocation(),
                    format("Unsupported type [%s] for provided value [%s]", token, context.parser().text())
                );
            };
        }

        @Override
        public int getNumBytes(int dimensions) {
            return dimensions;
        }

        @Override
        public ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
            return ByteBuffer.wrap(new byte[numBytes]);
        }

        static boolean isMaybeHexString(String s) {
            int len = s.length();
            if (len % 2 != 0) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                char c = s.charAt(i);
                if (HexFormat.isHexDigit(c) == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int parseDimensionCount(DocumentParserContext context) throws IOException {
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
                    String v = context.parser().text();
                    // Base64 is always divisible by 4, so if it's not try hex
                    if (v.length() % 4 != 0) {
                        try {
                            yield HexFormat.of().parseHex(v).length;
                        } catch (IllegalArgumentException e) {
                            yield Base64.getDecoder().decode(v).length;
                        }
                    } else {
                        try {
                            yield Base64.getDecoder().decode(v).length;
                        } catch (IllegalArgumentException e) {
                            yield HexFormat.of().parseHex(v).length;
                        }
                    }
                }
                default -> throw new ParsingException(
                    context.parser().getTokenLocation(),
                    format("Unsupported type [%s] for provided value [%s]", currentToken, context.parser().text())
                );
            };
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

        static UnaryOperator<StringBuilder> errorElementsAppender(byte[] vector) {
            return sb -> appendErrorElements(sb, vector);
        }
    }

    private static class FloatElement extends Element {

        @Override
        public ElementType elementType() {
            return ElementType.FLOAT;
        }

        @Override
        public void writeValues(ByteBuffer byteBuffer, float[] values) {
            byteBuffer.asFloatBuffer().put(values);
            byteBuffer.position(byteBuffer.position() + (values.length * Float.BYTES));
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
                elementType(),
                denseVectorFieldType.dims,
                denseVectorFieldType.indexed,
                denseVectorFieldType.isNormalized() && denseVectorFieldType.indexed ? r -> new FilterLeafReader(r) {
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
        void checkVectorMagnitude(VectorSimilarity similarity, UnaryOperator<StringBuilder> appender, float squaredMagnitude) {
            StringBuilder errorBuilder = null;

            if (Float.isNaN(squaredMagnitude) || Float.isInfinite(squaredMagnitude)) {
                errorBuilder = new StringBuilder(
                    "NaN or Infinite magnitude detected, this usually means the vector values are too extreme to fit within a float."
                );
            }
            if (errorBuilder != null) {
                throw new IllegalArgumentException(appender.apply(errorBuilder).toString());
            }

            if (similarity == VectorSimilarity.DOT_PRODUCT && isUnitVector(squaredMagnitude) == false) {
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
            return ESVectorUtil.dotProduct(vectorData.asFloatVector(), vectorData.asFloatVector());
        }

        @Override
        public int parseDimensionCount(DocumentParserContext context) throws IOException {
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
                    byte[] decodedVectorBytes = Base64.getDecoder().decode(context.parser().text());
                    if (decodedVectorBytes.length % Float.BYTES != 0) {
                        throw new ParsingException(
                            context.parser().getTokenLocation(),
                            "Failed to parse object: Base64 decoded vector byte length ["
                                + decodedVectorBytes.length
                                + "] is not a multiple of ["
                                + Float.BYTES
                                + "]"
                        );
                    }
                    yield decodedVectorBytes.length / Float.BYTES;
                }
                default -> throw new ParsingException(
                    context.parser().getTokenLocation(),
                    format("Unsupported type [%s] for provided value [%s]", currentToken, context.parser().text())
                );
            };
        }

        @Override
        public void parseKnnVectorAndIndex(DocumentParserContext context, DenseVectorFieldMapper fieldMapper) throws IOException {
            var vandm = parseFloatVectorInput(context, fieldMapper.fieldType().dims, (i, end) -> {
                if (end) {
                    fieldMapper.checkDimensionMatches(i, context);
                } else {
                    fieldMapper.checkDimensionExceeded(i, context);
                }
            });
            checkVectorBounds(vandm.vectorData.asFloatVector());
            checkVectorMagnitude(
                fieldMapper.fieldType().similarity,
                errorElementsAppender(vandm.vectorData().floatVector()),
                vandm.squaredMagnitude
            );
            float[] vector = vandm.vectorData.asFloatVector();
            if (fieldMapper.fieldType().isNormalized() && isUnitVector(vandm.squaredMagnitude) == false) {
                float length = (float) Math.sqrt(vandm.squaredMagnitude);
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
                fieldMapper.fieldType().similarity.vectorSimilarityFunction(fieldMapper.indexCreatedVersion, elementType())
            );
            context.doc().addWithKey(fieldMapper.fieldType().name(), field);
        }

        @Override
        public VectorData parseKnnVector(
            DocumentParserContext context,
            int dims,
            IntBooleanConsumer dimChecker,
            VectorSimilarity similarity
        ) throws IOException {
            var v = parseFloatVectorInput(context, dims, (i, end) -> {
                if (end) {
                    dimChecker.accept(i, true);
                } else {
                    dimChecker.accept(i, false);
                }
            });
            checkVectorBounds(v.vectorData.asFloatVector());
            checkVectorMagnitude(similarity, errorElementsAppender(v.vectorData.asFloatVector()), v.squaredMagnitude);
            return v.vectorData;
        }

        VectorDataAndMagnitude parseFloatVectorInput(DocumentParserContext context, int dims, IntBooleanConsumer dimChecker)
            throws IOException {
            XContentParser.Token token = context.parser().currentToken();
            return switch (token) {
                case START_ARRAY -> parseVectorArray(context, dimChecker, dims);
                case VALUE_STRING -> parseBase64EncodedVector(context, dimChecker, dims);
                default -> throw new ParsingException(
                    context.parser().getTokenLocation(),
                    format("Unsupported type [%s] for provided value [%s]", token, context.parser().text())
                );
            };
        }

        VectorDataAndMagnitude parseVectorArray(DocumentParserContext context, IntBooleanConsumer dimChecker, int dims) throws IOException {
            int index = 0;
            float[] vector = new float[dims];
            float squaredMagnitude = 0;
            for (XContentParser.Token token = context.parser().nextToken(); token != Token.END_ARRAY; token = context.parser()
                .nextToken()) {
                dimChecker.accept(index, false);
                ensureExpectedToken(Token.VALUE_NUMBER, token, context.parser());
                float value = context.parser().floatValue(true);
                vector[index++] = value;
                squaredMagnitude += value * value;
            }
            dimChecker.accept(index, true);
            return new VectorDataAndMagnitude(VectorData.fromFloats(vector), squaredMagnitude);
        }

        VectorDataAndMagnitude parseBase64EncodedVector(DocumentParserContext context, IntBooleanConsumer dimChecker, int dims)
            throws IOException {
            XContentString.UTF8Bytes utfBytes = context.parser().optimizedText().bytes();
            ByteBuffer srcBuffer = ByteBuffer.wrap(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
            // BIG_ENDIAN is the default, but just being explicit here
            ByteBuffer byteBuffer = Base64.getDecoder().decode(srcBuffer).order(ByteOrder.BIG_ENDIAN);
            float[] decodedVector = new float[dims];
            if (byteBuffer.remaining() == dims * Float.BYTES) {
                byteBuffer.asFloatBuffer().get(decodedVector);
            } else if (byteBuffer.remaining() == dims * BFloat16.BYTES) {
                BFloat16.bFloat16ToFloat(byteBuffer.asShortBuffer(), decodedVector);
            } else {
                throw new ParsingException(
                    context.parser().getTokenLocation(),
                    "Failed to parse object: Base64 decoded vector byte length ["
                        + byteBuffer.remaining()
                        + "] does not match the expected length of ["
                        + (dims * Float.BYTES)
                        + "] or ["
                        + (dims * BFloat16.BYTES)
                        + "] for dimension count ["
                        + dims
                        + "]"
                );
            }

            dimChecker.accept(decodedVector.length, true);
            VectorData vectorData = VectorData.fromFloats(decodedVector);
            float squaredMagnitude = (float) computeSquaredMagnitude(vectorData);
            return new VectorDataAndMagnitude(vectorData, squaredMagnitude);
        }

        record VectorDataAndMagnitude(VectorData vectorData, float squaredMagnitude) {}

        @Override
        public int getNumBytes(int dimensions) {
            return dimensions * Float.BYTES;
        }

        @Override
        public ByteBuffer createByteBuffer(IndexVersion indexVersion, int numBytes) {
            return indexVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)
                ? ByteBuffer.wrap(new byte[numBytes]).order(ByteOrder.LITTLE_ENDIAN)
                : ByteBuffer.wrap(new byte[numBytes]);
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

        static UnaryOperator<StringBuilder> errorElementsAppender(float[] vector) {
            return sb -> appendErrorElements(sb, vector);
        }
    }

    private static class BFloat16Element extends FloatElement {

        @Override
        public ElementType elementType() {
            return ElementType.BFLOAT16;
        }

        @Override
        public void writeValues(ByteBuffer byteBuffer, float[] values) {
            BFloat16.floatToBFloat16(values, byteBuffer.asShortBuffer());
            byteBuffer.position(byteBuffer.position() + (values.length * BFloat16.BYTES));
        }

        @Override
        public void readAndWriteValue(ByteBuffer byteBuffer, XContentBuilder b) throws IOException {
            b.value(BFloat16.bFloat16ToFloat(byteBuffer.getShort()));
        }

        @Override
        public boolean isUnitVector(float squaredMagnitude) {
            // bfloat16 needs to be more lenient
            return Math.abs(squaredMagnitude - 1.0f) < 0.02f;
        }

        @Override
        public int getNumBytes(int dimensions) {
            return dimensions * BFloat16.BYTES;
        }
    }

    private static class BitElement extends ByteElement {

        @Override
        public ElementType elementType() {
            return ElementType.BIT;
        }

        @Override
        void checkVectorMagnitude(VectorSimilarity similarity, UnaryOperator<StringBuilder> appender, float squaredMagnitude) {}

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

        @Override
        VectorData parseVectorArray(DocumentParserContext context, int dims, IntBooleanConsumer dimChecker, VectorSimilarity similarity)
            throws IOException {
            return super.parseVectorArray(
                context,
                dims / Byte.SIZE,
                (value, isComplete) -> dimChecker.accept(value * Byte.SIZE, isComplete),
                similarity
            );
        }

        @Override
        VectorData parseStringValue(
            String s,
            IntBooleanConsumer dimChecker,
            VectorSimilarity similarity,
            Function<String, byte[]> decoder
        ) {
            byte[] decodedVector = decoder.apply(s);
            dimChecker.accept(decodedVector.length * Byte.SIZE, true);
            return VectorData.fromBytes(decodedVector);
        }

        @Override
        public int getNumBytes(int dimensions) {
            assert dimensions % Byte.SIZE == 0;
            return dimensions / Byte.SIZE;
        }

        @Override
        public int parseDimensionCount(DocumentParserContext context) throws IOException {
            return super.parseDimensionCount(context) * Byte.SIZE;
        }

        @Override
        public void checkDimensions(Integer dvDims, int qvDims) {
            super.checkDimensions(dvDims, qvDims * Byte.SIZE);
        }
    }

    public enum VectorSimilarity {
        L2_NORM {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE, FLOAT, BFLOAT16 -> 1f / (1f + similarity * similarity);
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
                    case BYTE, FLOAT, BFLOAT16 -> (1 + similarity) / 2f;
                    default -> throw new IllegalArgumentException("Unsupported element type [" + elementType + "]");
                };
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction(IndexVersion indexVersion, ElementType elementType) {
                return indexVersion.onOrAfter(NORMALIZE_COSINE) && (elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16)
                    ? VectorSimilarityFunction.DOT_PRODUCT
                    : VectorSimilarityFunction.COSINE;
            }
        },
        DOT_PRODUCT {
            @Override
            float score(float similarity, ElementType elementType, int dim) {
                return switch (elementType) {
                    case BYTE -> 0.5f + similarity / (float) (dim * (1 << 15));
                    case FLOAT, BFLOAT16 -> (1 + similarity) / 2f;
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
                    case BYTE, FLOAT, BFLOAT16 -> similarity < 0 ? 1 / (1 + -1 * similarity) : similarity + 1;
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

    public abstract static class DenseVectorIndexOptions implements IndexOptions {
        final VectorIndexType type;

        DenseVectorIndexOptions(VectorIndexType type) {
            this.type = type;
        }

        abstract KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers);

        public boolean validate(ElementType elementType, int dim, boolean throwOnError) {
            return validateElementType(elementType, throwOnError) && validateDimension(dim, throwOnError);
        }

        public boolean validateElementType(ElementType elementType) {
            return validateElementType(elementType, true);
        }

        final boolean validateElementType(ElementType elementType, boolean throwOnError) {
            boolean validElementType = type.supportsElementType(elementType);
            if (throwOnError && validElementType == false) {
                throw new IllegalArgumentException(
                    "[element_type] cannot be [" + elementType.toString() + "] when using index type [" + type + "]"
                );
            }
            return validElementType;
        }

        public abstract boolean updatableTo(DenseVectorIndexOptions update);

        public boolean validateDimension(int dim) {
            return validateDimension(dim, true);
        }

        public boolean validateDimension(int dim, boolean throwOnError) {
            boolean supportsDimension = type.supportsDimension(dim);
            if (throwOnError && supportsDimension == false) {
                throw new IllegalArgumentException(type.name + " only supports even dimensions; provided=" + dim);
            }
            return supportsDimension;
        }

        abstract boolean doEquals(DenseVectorIndexOptions other);

        abstract int doHashCode();

        public VectorIndexType getType() {
            return type;
        }

        @Override
        public final boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null || other.getClass() != getClass()) {
                return false;
            }
            DenseVectorIndexOptions otherOptions = (DenseVectorIndexOptions) other;
            return Objects.equals(type, otherOptions.type) && doEquals(otherOptions);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(type, doHashCode());
        }

        /**
         * Indicates whether the underlying vector search is performed using a flat (exhaustive) approach.
         * <p>
         * When {@code true}, it means the search does not use any approximate nearest neighbor (ANN)
         * acceleration structures such as HNSW or IVF. Instead, it performs a brute-force comparison
         * against all candidate vectors. This information can be used by higher-level components
         * to decide whether additional acceleration or optimization is necessary.
         *
         * @return {@code true} if the vector search is flat (exhaustive), {@code false} if it uses ANN structures
         */
        public abstract boolean isFlat();
    }

    abstract static class QuantizedIndexOptions extends DenseVectorIndexOptions {
        final RescoreVector rescoreVector;

        QuantizedIndexOptions(VectorIndexType type, RescoreVector rescoreVector) {
            super(type);
            this.rescoreVector = rescoreVector;
        }
    }

    public enum VectorIndexType {
        HNSW("hnsw", false) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");

                int m = XContentMapValues.nodeIntegerValue(mNode, Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode, Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);
                if (onDiskRescore) {
                    throw new IllegalArgumentException("on_disk_rescore is only supported for indexed and quantized vector types");
                }
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
        INT8_HNSW("int8_hnsw", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");

                int m = XContentMapValues.nodeIntegerValue(mNode, Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode, Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);

                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                RescoreVector rescoreVector = null;
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int8HnswIndexOptions(m, efConstruction, confidenceInterval, onDiskRescore, rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        },
        INT4_HNSW("int4_hnsw", true) {
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");

                int m = XContentMapValues.nodeIntegerValue(mNode, Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode, Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);

                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                RescoreVector rescoreVector = null;
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);

                return new Int4HnswIndexOptions(m, efConstruction, confidenceInterval, onDiskRescore, rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims % 2 == 0;
            }
        },
        FLAT("flat", false) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);
                if (onDiskRescore) {
                    throw new IllegalArgumentException("on_disk_rescore is only supported for indexed and quantized vector types");
                }
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
        INT8_FLAT("int8_flat", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                RescoreVector rescoreVector = null;
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                }
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);
                if (onDiskRescore) {
                    throw new IllegalArgumentException("on_disk_rescore is only supported for indexed and quantized vector types");
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int8FlatIndexOptions(confidenceInterval, rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        },
        INT4_FLAT("int4_flat", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");
                Object confidenceIntervalNode = indexOptionsMap.remove("confidence_interval");
                Float confidenceInterval = null;
                if (confidenceIntervalNode != null) {
                    confidenceInterval = (float) XContentMapValues.nodeDoubleValue(confidenceIntervalNode);
                }
                RescoreVector rescoreVector = null;
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                }
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);
                if (onDiskRescore) {
                    throw new IllegalArgumentException("on_disk_rescore is only supported for indexed and quantized vector types");
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new Int4FlatIndexOptions(confidenceInterval, rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims % 2 == 0;
            }
        },
        BBQ_HNSW("bbq_hnsw", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object mNode = indexOptionsMap.remove("m");
                Object efConstructionNode = indexOptionsMap.remove("ef_construction");
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");

                int m = XContentMapValues.nodeIntegerValue(mNode, Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN);
                int efConstruction = XContentMapValues.nodeIntegerValue(efConstructionNode, Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH);
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);

                RescoreVector rescoreVector = null;
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                    if (rescoreVector == null && defaultOversampleForBBQ(indexVersion)) {
                        rescoreVector = new RescoreVector(DEFAULT_OVERSAMPLE);
                    }
                }

                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new BBQHnswIndexOptions(m, efConstruction, onDiskRescore, rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims >= BBQ_MIN_DIMS;
            }
        },
        BBQ_FLAT("bbq_flat", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                RescoreVector rescoreVector = null;
                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");
                if (hasRescoreIndexVersion(indexVersion)) {
                    rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);
                    if (rescoreVector == null && defaultOversampleForBBQ(indexVersion)) {
                        rescoreVector = new RescoreVector(DEFAULT_OVERSAMPLE);
                    }
                }
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);
                if (onDiskRescore) {
                    throw new IllegalArgumentException("on_disk_rescore is only supported for indexed and quantized vector types");
                }
                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new BBQFlatIndexOptions(rescoreVector);
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return dims >= BBQ_MIN_DIMS;
            }
        },
        BBQ_DISK("bbq_disk", true) {
            @Override
            public DenseVectorIndexOptions parseIndexOptions(String fieldName, Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
                Object clusterSizeNode = indexOptionsMap.remove("cluster_size");
                int clusterSize = ES920DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER;
                if (clusterSizeNode != null) {
                    clusterSize = XContentMapValues.nodeIntegerValue(clusterSizeNode);
                    if (clusterSize < MIN_VECTORS_PER_CLUSTER || clusterSize > MAX_VECTORS_PER_CLUSTER) {
                        throw new IllegalArgumentException(
                            "cluster_size must be between "
                                + MIN_VECTORS_PER_CLUSTER
                                + " and "
                                + MAX_VECTORS_PER_CLUSTER
                                + ", got: "
                                + clusterSize
                        );
                    }
                }

                RescoreVector rescoreVector = RescoreVector.fromIndexOptions(indexOptionsMap, indexVersion);

                Object visitPercentageNode = indexOptionsMap.remove("default_visit_percentage");
                double visitPercentage = 0d;
                if (visitPercentageNode != null) {
                    visitPercentage = (float) XContentMapValues.nodeDoubleValue(visitPercentageNode);
                    if (visitPercentage < 0d || visitPercentage > 100d) {
                        throw new IllegalArgumentException(
                            "default_visit_percentage must be between 0.0 and 100.0, got: "
                                + visitPercentage
                                + " for field ["
                                + fieldName
                                + "]"
                        );
                    }
                }

                Object onDiskRescoreNode = indexOptionsMap.remove("on_disk_rescore");
                boolean onDiskRescore = XContentMapValues.nodeBooleanValue(onDiskRescoreNode, false);

                int quantizeBits;
                if (indexVersion.onOrAfter(DISK_BBQ_QUANTIZE_BITS) && Build.current().isSnapshot()) {
                    Object quantizeBitsNode = indexOptionsMap.remove("bits");
                    quantizeBits = XContentMapValues.nodeIntegerValue(quantizeBitsNode, DEFAULT_BBQ_IVF_QUANTIZE_BITS);
                    if ((quantizeBits == 1 || quantizeBits == 2 || quantizeBits == 4) == false) {
                        throw new IllegalArgumentException(
                            "'bits' must be 1, 2 or 4, got: " + quantizeBits + " for field [" + fieldName + "]"
                        );
                    }
                } else {
                    quantizeBits = 1;
                }
                if (rescoreVector == null) {
                    // adjust the oversampling factor based on quantization scheme
                    // no oversampling with 4 bits
                    if (quantizeBits == 2) {
                        rescoreVector = new RescoreVector(1.5f);
                    } else if (quantizeBits == 1) {
                        rescoreVector = new RescoreVector(DEFAULT_OVERSAMPLE);
                    }
                }

                boolean doPrecondition = false;
                if (Build.current().isSnapshot()) {
                    doPrecondition = XContentMapValues.nodeBooleanValue(indexOptionsMap.remove("precondition"), false);
                }

                MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
                return new BBQIVFIndexOptions(
                    clusterSize,
                    visitPercentage,
                    onDiskRescore,
                    rescoreVector,
                    indexVersion,
                    doPrecondition,
                    quantizeBits
                );
            }

            @Override
            public boolean supportsElementType(ElementType elementType) {
                return elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            }

            @Override
            public boolean supportsDimension(int dims) {
                return true;
            }
        };

        public static Optional<VectorIndexType> fromString(String type) {
            return Stream.of(VectorIndexType.values()).filter(vectorIndexType -> vectorIndexType.name.equals(type)).findFirst();
        }

        private final String name;
        private final boolean quantized;

        VectorIndexType(String name, boolean quantized) {
            this.name = name;
            this.quantized = quantized;
        }

        public abstract DenseVectorIndexOptions parseIndexOptions(
            String fieldName,
            Map<String, ?> indexOptionsMap,
            IndexVersion indexVersion
        );

        public abstract boolean supportsElementType(ElementType elementType);

        public abstract boolean supportsDimension(int dims);

        public boolean isQuantized() {
            return quantized;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static class Int8FlatIndexOptions extends QuantizedIndexOptions {
        private final Float confidenceInterval;

        Int8FlatIndexOptions(Float confidenceInterval, RescoreVector rescoreVector) {
            super(VectorIndexType.INT8_FLAT, rescoreVector);
            this.confidenceInterval = confidenceInterval;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            if (confidenceInterval != null) {
                builder.field("confidence_interval", confidenceInterval);
            }
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93ScalarQuantizedVectorsFormat(elementType, confidenceInterval, 7, false, false);
        }

        @Override
        boolean doEquals(DenseVectorIndexOptions o) {
            Int8FlatIndexOptions that = (Int8FlatIndexOptions) o;
            return Objects.equals(confidenceInterval, that.confidenceInterval) && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        int doHashCode() {
            return Objects.hash(confidenceInterval, rescoreVector);
        }

        @Override
        public boolean isFlat() {
            return true;
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            return update.type.equals(this.type)
                || update.type.equals(VectorIndexType.HNSW)
                || update.type.equals(VectorIndexType.INT8_HNSW)
                || update.type.equals(VectorIndexType.INT4_HNSW)
                || update.type.equals(VectorIndexType.BBQ_HNSW)
                || update.type.equals(VectorIndexType.INT4_FLAT)
                || update.type.equals(VectorIndexType.BBQ_FLAT);
        }
    }

    static class FlatIndexOptions extends DenseVectorIndexOptions {

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
        KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            return new ES93FlatVectorFormat(elementType);
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            return true;
        }

        @Override
        public boolean doEquals(DenseVectorIndexOptions o) {
            return o instanceof FlatIndexOptions;
        }

        @Override
        public int doHashCode() {
            return Objects.hash(type);
        }

        @Override
        public boolean isFlat() {
            return true;
        }
    }

    public static class Int4HnswIndexOptions extends QuantizedIndexOptions {
        private final int m;
        private final int efConstruction;
        private final float confidenceInterval;
        private final boolean onDiskRescore;

        public Int4HnswIndexOptions(
            int m,
            int efConstruction,
            Float confidenceInterval,
            boolean onDiskRescore,
            RescoreVector rescoreVector
        ) {
            super(VectorIndexType.INT4_HNSW, rescoreVector);
            this.m = m;
            this.efConstruction = efConstruction;
            // The default confidence interval for int4 is dynamic quantiles, this provides the best relevancy and is
            // effectively required for int4 to behave well across a wide range of data.
            this.confidenceInterval = confidenceInterval == null ? 0f : confidenceInterval;
            this.onDiskRescore = onDiskRescore;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93HnswScalarQuantizedVectorsFormat(
                m,
                efConstruction,
                elementType,
                confidenceInterval,
                4,
                true,
                onDiskRescore,
                numMergeWorkers,
                mergingExecutorService
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            builder.field("confidence_interval", confidenceInterval);
            if (onDiskRescore) {
                builder.field("on_disk_rescore", true);
            }
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean doEquals(DenseVectorIndexOptions o) {
            Int4HnswIndexOptions that = (Int4HnswIndexOptions) o;
            return m == that.m
                && efConstruction == that.efConstruction
                && Objects.equals(confidenceInterval, that.confidenceInterval)
                && onDiskRescore == that.onDiskRescore
                && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        public int doHashCode() {
            return Objects.hash(m, efConstruction, confidenceInterval, onDiskRescore, rescoreVector);
        }

        @Override
        public boolean isFlat() {
            return false;
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
                + ", on_disk_rescore="
                + onDiskRescore
                + ", rescore_vector="
                + (rescoreVector == null ? "none" : rescoreVector)
                + "}";
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            boolean updatable = false;
            if (update.type.equals(VectorIndexType.INT4_HNSW)) {
                Int4HnswIndexOptions int4HnswIndexOptions = (Int4HnswIndexOptions) update;
                // fewer connections would break assumptions on max number of connections (based on largest previous graph) during merge
                // quantization could not behave as expected with different confidence intervals (and quantiles) to be created
                updatable = int4HnswIndexOptions.m >= this.m && confidenceInterval == int4HnswIndexOptions.confidenceInterval;
            } else if (update.type.equals(VectorIndexType.BBQ_HNSW)) {
                updatable = ((BBQHnswIndexOptions) update).m >= m;
            }
            return updatable;
        }
    }

    static class Int4FlatIndexOptions extends QuantizedIndexOptions {
        private final float confidenceInterval;

        Int4FlatIndexOptions(Float confidenceInterval, RescoreVector rescoreVector) {
            super(VectorIndexType.INT4_FLAT, rescoreVector);
            // The default confidence interval for int4 is dynamic quantiles, this provides the best relevancy and is
            // effectively required for int4 to behave well across a wide range of data.
            this.confidenceInterval = confidenceInterval == null ? 0f : confidenceInterval;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93ScalarQuantizedVectorsFormat(elementType, confidenceInterval, 4, true, false);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("confidence_interval", confidenceInterval);
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean doEquals(DenseVectorIndexOptions o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Int4FlatIndexOptions that = (Int4FlatIndexOptions) o;
            return Objects.equals(confidenceInterval, that.confidenceInterval) && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        public int doHashCode() {
            return Objects.hash(confidenceInterval, rescoreVector);
        }

        @Override
        public boolean isFlat() {
            return true;
        }

        @Override
        public String toString() {
            return "{type=" + type + ", confidence_interval=" + confidenceInterval + ", rescore_vector=" + rescoreVector + "}";
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            // TODO: add support for updating from flat, hnsw, and int8_hnsw and updating params
            return update.type.equals(this.type)
                || update.type.equals(VectorIndexType.HNSW)
                || update.type.equals(VectorIndexType.INT8_HNSW)
                || update.type.equals(VectorIndexType.INT4_HNSW)
                || update.type.equals(VectorIndexType.BBQ_HNSW)
                || update.type.equals(VectorIndexType.BBQ_FLAT);
        }
    }

    public static class Int8HnswIndexOptions extends QuantizedIndexOptions {
        private final int m;
        private final int efConstruction;
        private final Float confidenceInterval;
        private final boolean onDiskRescore;

        public Int8HnswIndexOptions(
            int m,
            int efConstruction,
            Float confidenceInterval,
            boolean onDiskRescore,
            RescoreVector rescoreVector
        ) {
            super(VectorIndexType.INT8_HNSW, rescoreVector);
            this.m = m;
            this.efConstruction = efConstruction;
            this.confidenceInterval = confidenceInterval;
            this.onDiskRescore = onDiskRescore;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93HnswScalarQuantizedVectorsFormat(
                m,
                efConstruction,
                elementType,
                confidenceInterval,
                7,
                false,
                onDiskRescore,
                numMergeWorkers,
                mergingExecutorService
            );
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
            if (onDiskRescore) {
                builder.field("on_disk_rescore", true);
            }
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean doEquals(DenseVectorIndexOptions o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Int8HnswIndexOptions that = (Int8HnswIndexOptions) o;
            return m == that.m
                && efConstruction == that.efConstruction
                && Objects.equals(confidenceInterval, that.confidenceInterval)
                && onDiskRescore == that.onDiskRescore
                && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        public int doHashCode() {
            return Objects.hash(m, efConstruction, confidenceInterval, onDiskRescore, rescoreVector);
        }

        @Override
        public boolean isFlat() {
            return false;
        }

        public int m() {
            return m;
        }

        public int efConstruction() {
            return efConstruction;
        }

        public Float confidenceInterval() {
            return confidenceInterval;
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
                + ", on_disk_rescore="
                + onDiskRescore
                + ", rescore_vector="
                + (rescoreVector == null ? "none" : rescoreVector)
                + "}";
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
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
                updatable = update.type.equals(VectorIndexType.INT4_HNSW) && ((Int4HnswIndexOptions) update).m >= this.m
                    || (update.type.equals(VectorIndexType.BBQ_HNSW) && ((BBQHnswIndexOptions) update).m >= m);
            }
            return updatable;
        }
    }

    public static class HnswIndexOptions extends DenseVectorIndexOptions {
        private final int m;
        private final int efConstruction;

        HnswIndexOptions(int m, int efConstruction) {
            super(VectorIndexType.HNSW);
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            return new ES93HnswVectorsFormat(m, efConstruction, elementType, numMergeWorkers, mergingExecutorService);
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            boolean updatable = update.type.equals(this.type);
            if (updatable) {
                // fewer connections would break assumptions on max number of connections (based on largest previous graph) during merge
                HnswIndexOptions hnswIndexOptions = (HnswIndexOptions) update;
                updatable = hnswIndexOptions.m >= this.m;
            }
            return updatable
                || (update.type.equals(VectorIndexType.INT8_HNSW) && ((Int8HnswIndexOptions) update).m >= m)
                || (update.type.equals(VectorIndexType.INT4_HNSW) && ((Int4HnswIndexOptions) update).m >= m)
                || (update.type.equals(VectorIndexType.BBQ_HNSW) && ((BBQHnswIndexOptions) update).m >= m);
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
        public boolean doEquals(DenseVectorIndexOptions o) {
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
        public boolean isFlat() {
            return false;
        }

        public int m() {
            return m;
        }

        public int efConstruction() {
            return efConstruction;
        }

        @Override
        public String toString() {
            return "{type=" + type + ", m=" + m + ", ef_construction=" + efConstruction + "}";
        }
    }

    public static class BBQHnswIndexOptions extends QuantizedIndexOptions {
        private final int m;
        private final int efConstruction;
        private final boolean onDiskRescore;

        public BBQHnswIndexOptions(int m, int efConstruction, boolean onDiskRescore, RescoreVector rescoreVector) {
            super(VectorIndexType.BBQ_HNSW, rescoreVector);
            this.m = m;
            this.efConstruction = efConstruction;
            this.onDiskRescore = onDiskRescore;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93HnswBinaryQuantizedVectorsFormat(
                m,
                efConstruction,
                elementType,
                onDiskRescore,
                numMergeWorkers,
                mergingExecutorService
            );
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            return update.type.equals(this.type) && ((BBQHnswIndexOptions) update).m >= this.m;
        }

        @Override
        boolean doEquals(DenseVectorIndexOptions other) {
            BBQHnswIndexOptions that = (BBQHnswIndexOptions) other;
            return m == that.m
                && efConstruction == that.efConstruction
                && onDiskRescore == that.onDiskRescore
                && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        int doHashCode() {
            return Objects.hash(m, efConstruction, onDiskRescore, rescoreVector);
        }

        @Override
        public boolean isFlat() {
            return false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            if (onDiskRescore) {
                builder.field("on_disk_rescore", true);
            }
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean validateDimension(int dim, boolean throwOnError) {
            boolean supportsDimension = type.supportsDimension(dim);
            if (throwOnError && supportsDimension == false) {
                throw new IllegalArgumentException(
                    type.name + " does not support dimensions fewer than " + BBQ_MIN_DIMS + "; provided=" + dim
                );
            }
            return supportsDimension;
        }
    }

    static class BBQFlatIndexOptions extends QuantizedIndexOptions {
        private final int CLASS_NAME_HASH = this.getClass().getName().hashCode();

        BBQFlatIndexOptions(RescoreVector rescoreVector) {
            super(VectorIndexType.BBQ_FLAT, rescoreVector);
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            return new ES93BinaryQuantizedVectorsFormat(elementType, false);
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            return update.type.equals(this.type) || update.type.equals(VectorIndexType.BBQ_HNSW);
        }

        @Override
        boolean doEquals(DenseVectorIndexOptions other) {
            return other instanceof BBQFlatIndexOptions;
        }

        @Override
        int doHashCode() {
            return CLASS_NAME_HASH;
        }

        @Override
        public boolean isFlat() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean validateDimension(int dim, boolean throwOnError) {
            boolean supportsDimension = type.supportsDimension(dim);
            if (throwOnError && supportsDimension == false) {
                throw new IllegalArgumentException(
                    type.name + " does not support dimensions fewer than " + BBQ_MIN_DIMS + "; provided=" + dim
                );
            }
            return supportsDimension;
        }

    }

    public static class BBQIVFIndexOptions extends QuantizedIndexOptions {
        final int clusterSize;
        final double defaultVisitPercentage;
        final boolean onDiskRescore;
        final IndexVersion indexVersionCreated;
        final int bits;
        final boolean doPrecondition;

        BBQIVFIndexOptions(
            int clusterSize,
            double defaultVisitPercentage,
            boolean onDiskRescore,
            RescoreVector rescoreVector,
            IndexVersion indexVersionCreated,
            boolean doPrecondition,
            int bits
        ) {
            super(VectorIndexType.BBQ_DISK, rescoreVector);
            this.clusterSize = clusterSize;
            this.defaultVisitPercentage = defaultVisitPercentage;
            this.onDiskRescore = onDiskRescore;
            this.indexVersionCreated = indexVersionCreated;
            this.bits = bits;
            this.doPrecondition = doPrecondition;
        }

        @Override
        KnnVectorsFormat getVectorsFormat(ElementType elementType, ExecutorService mergingExecutorService, int numMergeWorkers) {
            assert elementType == ElementType.FLOAT || elementType == ElementType.BFLOAT16;
            if (indexVersionCreated.onOrAfter(IndexVersions.DISK_BBQ_LICENSE_ENFORCEMENT)) {
                // if we got here, this means we didn't get the plugin installed, so we should throw an exception
                throw new ElasticsearchSecurityException(
                    "current license is non-compliant for [{}]",
                    RestStatus.FORBIDDEN,
                    VectorIndexType.BBQ_DISK.name
                );
            }
            if (Build.current().isSnapshot()) {
                return new ESNextDiskBBQVectorsFormat(
                    ESNextDiskBBQVectorsFormat.QuantEncoding.fromId(bits >> 1),
                    clusterSize,
                    ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                    elementType,
                    onDiskRescore,
                    mergingExecutorService,
                    numMergeWorkers,
                    doPrecondition,
                    ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION
                );
            }
            return new ES920DiskBBQVectorsFormat(
                clusterSize,
                ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                elementType,
                onDiskRescore,
                mergingExecutorService,
                numMergeWorkers
            );
        }

        @Override
        public boolean updatableTo(DenseVectorIndexOptions update) {
            return update.type.equals(this.type);
        }

        @Override
        boolean doEquals(DenseVectorIndexOptions other) {
            BBQIVFIndexOptions that = (BBQIVFIndexOptions) other;
            return clusterSize == that.clusterSize
                && defaultVisitPercentage == that.defaultVisitPercentage
                && onDiskRescore == that.onDiskRescore
                && bits == that.bits
                && Objects.equals(rescoreVector, that.rescoreVector);
        }

        @Override
        int doHashCode() {
            return Objects.hash(clusterSize, defaultVisitPercentage, onDiskRescore, rescoreVector, bits);
        }

        @Override
        public boolean isFlat() {
            return false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("cluster_size", clusterSize);
            builder.field("default_visit_percentage", defaultVisitPercentage);
            if (onDiskRescore) {
                builder.field("on_disk_rescore", true);
            }
            if (rescoreVector != null) {
                rescoreVector.toXContent(builder, params);
            }
            if (indexVersionCreated.onOrAfter(DISK_BBQ_QUANTIZE_BITS) && Build.current().isSnapshot()) {
                builder.field("bits", bits);
            }
            if (doPrecondition) {
                builder.field("precondition", doPrecondition);
            }
            builder.endObject();
            return builder;
        }

        public int getClusterSize() {
            return clusterSize;
        }

        public double getDefaultVisitPercentage() {
            return defaultVisitPercentage;
        }

        public boolean isOnDiskRescore() {
            return onDiskRescore;
        }

        public boolean doPrecondition() {
            return doPrecondition;
        }
    }

    public record RescoreVector(float oversample) implements ToXContentObject {
        static final String NAME = "rescore_vector";
        static final String OVERSAMPLE = "oversample";

        static RescoreVector fromIndexOptions(Map<String, ?> indexOptionsMap, IndexVersion indexVersion) {
            Object rescoreVectorNode = indexOptionsMap.remove(NAME);
            if (rescoreVectorNode == null) {
                return null;
            }
            Map<String, Object> mappedNode = XContentMapValues.nodeMapValue(rescoreVectorNode, NAME);
            Object oversampleNode = mappedNode.get(OVERSAMPLE);
            if (oversampleNode == null) {
                throw new IllegalArgumentException("Invalid rescore_vector value. Missing required field " + OVERSAMPLE);
            }
            float oversampleValue = (float) XContentMapValues.nodeDoubleValue(oversampleNode);
            if (oversampleValue == 0 && allowsZeroRescore(indexVersion) == false) {
                throw new IllegalArgumentException("oversample must be greater than 1");
            }
            if (oversampleValue < 1 && oversampleValue != 0) {
                throw new IllegalArgumentException("oversample must be greater than 1 or exactly 0");
            } else if (oversampleValue > 10) {
                throw new IllegalArgumentException("oversample must be less than or equal to 10");
            }
            return new RescoreVector(oversampleValue);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field(OVERSAMPLE, oversample);
            builder.endObject();
            return builder;
        }
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(
            n,
            c.getIndexSettings().getIndexVersionCreated(),
            INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(c.getIndexSettings().getSettings()),
            c.getVectorsFormatProviders()
        ),
        notInMultiFields(CONTENT_TYPE)
    );

    public static final class DenseVectorFieldType extends SimpleMappedFieldType {
        private final Element element;
        private final Integer dims;
        private final boolean indexed;
        private final VectorSimilarity similarity;
        private final IndexVersion indexVersionCreated;
        private final DenseVectorIndexOptions indexOptions;
        private final boolean isSyntheticSource;

        public DenseVectorFieldType(
            String name,
            IndexVersion indexVersionCreated,
            ElementType elementType,
            Integer dims,
            boolean indexed,
            VectorSimilarity similarity,
            DenseVectorIndexOptions indexOptions,
            Map<String, String> meta,
            boolean isSyntheticSource
        ) {
            super(name, indexed ? IndexType.vectors() : IndexType.docValuesOnly(), false, meta);
            this.element = Element.getElement(elementType);
            this.dims = dims;
            this.indexed = indexed;
            this.similarity = similarity;
            this.indexVersionCreated = indexVersionCreated;
            this.indexOptions = indexOptions;
            this.isSyntheticSource = isSyntheticSource;
        }

        public VectorSimilarity similarity() {
            return similarity;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // TODO add support to `binary` and `vector` formats to unify the formats
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            Set<String> sourcePaths = context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet();
            return new SourceValueFetcher(name(), context) {
                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                    ArrayList<Object> values = new ArrayList<>();
                    for (var path : sourcePaths) {
                        Object sourceValue = source.extractValue(path, null);
                        if (sourceValue == null) {
                            return List.of();
                        }
                        switch (sourceValue) {
                            case List<?> v -> values.addAll(v);
                            case String s -> values.add(s);
                            default -> ignoredValues.add(sourceValue);
                        }
                    }
                    values.trimToSize();
                    return values;
                }

                @Override
                protected Object parseSourceValue(Object value) {
                    throw new IllegalStateException("parsing dense vector from source is not supported here");
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return switch (format) {
                case null -> DocValueFormat.DENSE_VECTOR;
                case "array" -> DocValueFormat.DENSE_VECTOR;
                case "binary" -> DocValueFormat.BINARY;
                default -> throw new IllegalArgumentException(
                    "Field ["
                        + name()
                        + "] of type ["
                        + typeName()
                        + "] doesn't support format ["
                        + format
                        + "]. Supported formats are [array, binary]."
                );
            };
        }

        @Override
        public boolean isSearchable() {
            return indexed;
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public boolean isVectorEmbedding() {
            return true;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return element.fielddataBuilder(this, fieldDataContext);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        public VectorData resolveQueryVector(VectorData queryVector) {
            if (queryVector == null || queryVector.isStringVector() == false) {
                return queryVector;
            }
            return VectorData.decodeQueryVector(queryVector.stringVector(), element.elementType(), dims);
        }

        public Query createExactKnnQuery(VectorData queryVector, Float vectorSimilarity) {
            if (indexType() == IndexType.NONE) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }
            if (dims == null) {
                return new MatchNoDocsQuery("No data has been indexed for field [" + name() + "]");
            }
            VectorData resolvedQueryVector = resolveQueryVector(queryVector);
            Query knnQuery = switch (element.elementType()) {
                case BYTE -> createExactKnnByteQuery(resolvedQueryVector.asByteVector());
                case FLOAT, BFLOAT16 -> createExactKnnFloatQuery(resolvedQueryVector.asFloatVector());
                case BIT -> createExactKnnBitQuery(resolvedQueryVector.asByteVector());
            };
            if (vectorSimilarity != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    vectorSimilarity,
                    similarity.score(vectorSimilarity, element.elementType(), dims)
                );
            }
            return knnQuery;
        }

        public boolean isNormalized() {
            return indexVersionCreated.onOrAfter(NORMALIZE_COSINE) && VectorSimilarity.COSINE.equals(similarity);
        }

        private Query createExactKnnBitQuery(byte[] queryVector) {
            element.checkDimensions(dims, queryVector.length);
            return new DenseVectorQuery.Bytes(queryVector, name());
        }

        private Query createExactKnnByteQuery(byte[] queryVector) {
            element.checkDimensions(dims, queryVector.length);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = ESVectorUtil.dotProduct(queryVector, queryVector);
                element.checkVectorMagnitude(similarity, ByteElement.errorElementsAppender(queryVector), squaredMagnitude);
            }
            return new DenseVectorQuery.Bytes(queryVector, name());
        }

        private Query createExactKnnFloatQuery(float[] queryVector) {
            element.checkDimensions(dims, queryVector.length);
            element.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = ESVectorUtil.dotProduct(queryVector, queryVector);
                element.checkVectorMagnitude(similarity, FloatElement.errorElementsAppender(queryVector), squaredMagnitude);
                if (isNormalized() && element.isUnitVector(squaredMagnitude) == false) {
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
            int k,
            int numCands,
            Float visitPercentage,
            Float oversample,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter,
            FilterHeuristic heuristic,
            boolean hnswEarlyTermination
        ) {
            if (indexType.hasVectors() == false) {
                throw new IllegalArgumentException(
                    "to perform knn search on field [" + name() + "], its mapping must have [index] set to [true]"
                );
            }
            if (dims == null) {
                return new MatchNoDocsQuery("No data has been indexed for field [" + name() + "]");
            }
            VectorData resolvedQueryVector = resolveQueryVector(queryVector);
            KnnSearchStrategy knnSearchStrategy = heuristic.getKnnSearchStrategy();
            hnswEarlyTermination &= canApplyPatienceQuery();
            return switch (getElementType()) {
                case BYTE -> createKnnByteQuery(
                    resolvedQueryVector.asByteVector(),
                    k,
                    numCands,
                    filter,
                    similarityThreshold,
                    parentFilter,
                    knnSearchStrategy,
                    hnswEarlyTermination
                );
                case FLOAT, BFLOAT16 -> createKnnFloatQuery(
                    resolvedQueryVector.asFloatVector(),
                    k,
                    numCands,
                    visitPercentage,
                    oversample,
                    filter,
                    similarityThreshold,
                    parentFilter,
                    knnSearchStrategy,
                    hnswEarlyTermination
                );
                case BIT -> createKnnBitQuery(
                    resolvedQueryVector.asByteVector(),
                    k,
                    numCands,
                    filter,
                    similarityThreshold,
                    parentFilter,
                    knnSearchStrategy,
                    hnswEarlyTermination
                );
            };
        }

        private boolean needsRescore(Float rescoreOversample) {
            return rescoreOversample != null && rescoreOversample > 0 && isQuantized();
        }

        private boolean isQuantized() {
            return indexOptions != null && indexOptions.type != null && indexOptions.type.isQuantized();
        }

        private boolean canApplyPatienceQuery() {
            return indexOptions instanceof HnswIndexOptions
                || indexOptions instanceof Int8HnswIndexOptions
                || indexOptions instanceof Int4HnswIndexOptions
                || indexOptions instanceof BBQHnswIndexOptions;
        }

        private Query createKnnBitQuery(
            byte[] queryVector,
            int k,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter,
            KnnSearchStrategy searchStrategy,
            boolean hnswEarlyTermination
        ) {
            element.checkDimensions(dims, queryVector.length);
            Query knnQuery;
            if (indexOptions != null && indexOptions.isFlat()) {
                var exactKnnQuery = parentFilter != null
                    ? new DiversifyingParentBlockQuery(parentFilter, createExactKnnBitQuery(queryVector))
                    : createExactKnnBitQuery(queryVector);
                knnQuery = filter == null
                    ? exactKnnQuery
                    : new BooleanQuery.Builder().add(exactKnnQuery, BooleanClause.Occur.SHOULD)
                        .add(filter, BooleanClause.Occur.FILTER)
                        .build();
            } else {
                knnQuery = parentFilter != null
                    ? new ESDiversifyingChildrenByteKnnVectorQuery(
                        name(),
                        queryVector,
                        filter,
                        k,
                        numCands,
                        parentFilter,
                        searchStrategy,
                        hnswEarlyTermination
                    )
                    : new ESKnnByteVectorQuery(name(), queryVector, k, numCands, filter, searchStrategy, hnswEarlyTermination);
            }
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, element.elementType(), dims)
                );
            }
            return knnQuery;
        }

        private Query createKnnByteQuery(
            byte[] queryVector,
            int k,
            int numCands,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter,
            KnnSearchStrategy searchStrategy,
            boolean hnswEarlyTermination
        ) {
            element.checkDimensions(dims, queryVector.length);

            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = ESVectorUtil.dotProduct(queryVector, queryVector);
                element.checkVectorMagnitude(similarity, ByteElement.errorElementsAppender(queryVector), squaredMagnitude);
            }
            Query knnQuery;
            if (indexOptions != null && indexOptions.isFlat()) {
                var exactKnnQuery = parentFilter != null
                    ? new DiversifyingParentBlockQuery(parentFilter, createExactKnnByteQuery(queryVector))
                    : createExactKnnByteQuery(queryVector);
                knnQuery = filter == null
                    ? exactKnnQuery
                    : new BooleanQuery.Builder().add(exactKnnQuery, BooleanClause.Occur.SHOULD)
                        .add(filter, BooleanClause.Occur.FILTER)
                        .build();
            } else {
                knnQuery = parentFilter != null
                    ? new ESDiversifyingChildrenByteKnnVectorQuery(
                        name(),
                        queryVector,
                        filter,
                        k,
                        numCands,
                        parentFilter,
                        searchStrategy,
                        hnswEarlyTermination
                    )
                    : new ESKnnByteVectorQuery(name(), queryVector, k, numCands, filter, searchStrategy, hnswEarlyTermination);
            }
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, element.elementType(), dims)
                );
            }
            return knnQuery;
        }

        private Query createKnnFloatQuery(
            float[] queryVector,
            int k,
            int numCands,
            Float visitPercentage,
            Float queryOversample,
            Query filter,
            Float similarityThreshold,
            BitSetProducer parentFilter,
            KnnSearchStrategy knnSearchStrategy,
            boolean hnswEarlyTermination
        ) {
            element.checkDimensions(dims, queryVector.length);
            element.checkVectorBounds(queryVector);
            if (similarity == VectorSimilarity.DOT_PRODUCT || similarity == VectorSimilarity.COSINE) {
                float squaredMagnitude = ESVectorUtil.dotProduct(queryVector, queryVector);
                element.checkVectorMagnitude(similarity, FloatElement.errorElementsAppender(queryVector), squaredMagnitude);
                if (isNormalized() && element.isUnitVector(squaredMagnitude) == false) {
                    float length = (float) Math.sqrt(squaredMagnitude);
                    queryVector = Arrays.copyOf(queryVector, queryVector.length);
                    for (int i = 0; i < queryVector.length; i++) {
                        queryVector[i] /= length;
                    }
                }
            }

            int adjustedK = k;
            // By default utilize the quantized oversample is configured
            // allow the user provided at query time overwrite
            Float oversample = queryOversample;
            if (oversample == null
                && indexOptions instanceof QuantizedIndexOptions quantizedIndexOptions
                && quantizedIndexOptions.rescoreVector != null) {
                oversample = quantizedIndexOptions.rescoreVector.oversample;
            }
            boolean rescore = needsRescore(oversample);
            if (rescore) {
                // Will get k * oversample for rescoring, and get the top k
                adjustedK = Math.min((int) Math.ceil(k * oversample), OVERSAMPLE_LIMIT);
                numCands = Math.max(adjustedK, numCands);
            }
            Query knnQuery;
            if (indexOptions != null && indexOptions.isFlat()) {
                var exactKnnQuery = parentFilter != null
                    ? new DiversifyingParentBlockQuery(parentFilter, createExactKnnFloatQuery(queryVector))
                    : createExactKnnFloatQuery(queryVector);
                knnQuery = filter == null
                    ? exactKnnQuery
                    : new BooleanQuery.Builder().add(exactKnnQuery, BooleanClause.Occur.SHOULD)
                        .add(filter, BooleanClause.Occur.FILTER)
                        .build();
            } else if (indexOptions instanceof BBQIVFIndexOptions bbqIndexOptions) {
                float defaultVisitRatio = (float) (bbqIndexOptions.defaultVisitPercentage / 100d);
                float visitRatio = visitPercentage == null ? defaultVisitRatio : (float) (visitPercentage / 100d);
                knnQuery = parentFilter != null
                    ? new DiversifyingChildrenIVFKnnFloatVectorQuery(
                        name(),
                        queryVector,
                        adjustedK,
                        numCands,
                        filter,
                        parentFilter,
                        visitRatio,
                        bbqIndexOptions.doPrecondition()
                    )
                    : new IVFKnnFloatVectorQuery(
                        name(),
                        queryVector,
                        adjustedK,
                        numCands,
                        filter,
                        visitRatio,
                        bbqIndexOptions.doPrecondition()
                    );
            } else {
                knnQuery = parentFilter != null
                    ? new ESDiversifyingChildrenFloatKnnVectorQuery(
                        name(),
                        queryVector,
                        filter,
                        adjustedK,
                        numCands,
                        parentFilter,
                        knnSearchStrategy
                    )
                    : new ESKnnFloatVectorQuery(name(), queryVector, adjustedK, numCands, filter, knnSearchStrategy, hnswEarlyTermination);
            }
            if (rescore) {
                knnQuery = RescoreKnnVectorQuery.fromInnerQuery(
                    name(),
                    queryVector,
                    similarity.vectorSimilarityFunction(indexVersionCreated, ElementType.FLOAT),
                    k,
                    adjustedK,
                    knnQuery
                );
            }
            if (similarityThreshold != null) {
                knnQuery = new VectorSimilarityQuery(
                    knnQuery,
                    similarityThreshold,
                    similarity.score(similarityThreshold, element.elementType(), dims)
                );
            }
            return knnQuery;
        }

        public VectorSimilarity getSimilarity() {
            return similarity;
        }

        public int getVectorDimensions() {
            return dims;
        }

        public ElementType getElementType() {
            return element.elementType();
        }

        public DenseVectorIndexOptions getIndexOptions() {
            return indexOptions;
        }

        @Override
        public BlockLoader blockLoader(MappedFieldType.BlockLoaderContext blContext) {
            if (dims == null) {
                // No data has been indexed yet
                return ConstantNull.INSTANCE;
            }

            BlockLoaderFunctionConfig cfg = blContext.blockLoaderFunctionConfig();
            if (indexed) {
                if (cfg == null) {
                    return new DenseVectorBlockLoader<>(
                        name(),
                        dims,
                        this,
                        new DenseVectorBlockLoaderProcessor.DenseVectorLoaderProcessor()
                    );
                }
                return switch (cfg.function()) {
                    case V_COSINE, V_DOT_PRODUCT, V_HAMMING, V_L1NORM, V_L2NORM -> {
                        VectorSimilarityFunctionConfig similarityConfig = (VectorSimilarityFunctionConfig) cfg;
                        yield new DenseVectorBlockLoader<>(
                            name(),
                            dims,
                            this,
                            new DenseVectorBlockLoaderProcessor.DenseVectorSimilarityProcessor(similarityConfig)
                        );
                    }
                    default -> throw new UnsupportedOperationException("Unknown block loader function config: " + cfg.function());
                };
            }

            if (cfg != null) {
                throw new IllegalArgumentException(
                    "Field ["
                        + name()
                        + "] of type ["
                        + typeName()
                        + "] doesn't support block loader functions when [index] is set to [false]"
                );
            }

            if (hasDocValues() && (blContext.fieldExtractPreference() != FieldExtractPreference.STORED || isSyntheticSource)) {
                return new DenseVectorFromBinaryBlockLoader(name(), dims, indexVersionCreated, element.elementType());
            }
            BlockSourceReader.LeafIteratorLookup lookup = BlockSourceReader.lookupMatchingAll();
            return new BlockSourceReader.DenseVectorBlockLoader(
                sourceValueFetcher(blContext.sourcePaths(name()), blContext.indexSettings()),
                lookup,
                dims
            );
        }

        @Override
        public boolean supportsBlockLoaderConfig(BlockLoaderFunctionConfig config, FieldExtractPreference preference) {
            if (dims == null) {
                // No data has been indexed yet
                return true;
            }

            if (indexed) {
                return switch (config.function()) {
                    case V_COSINE, V_DOT_PRODUCT, V_HAMMING, V_L1NORM, V_L2NORM -> true;
                    default -> false;
                };
            }
            return false;
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths, IndexSettings indexSettings) {
            return new SourceValueFetcher(sourcePaths, null, indexSettings.getIgnoredSourceFormat()) {
                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                    ArrayList<Object> values = new ArrayList<>();
                    for (var path : sourcePaths) {
                        Object sourceValue = source.extractValue(path, null);
                        if (sourceValue == null) {
                            return List.of();
                        }
                        try {
                            switch (sourceValue) {
                                case List<?> v -> {
                                    for (Object o : v) {
                                        values.add(NumberFieldMapper.NumberType.FLOAT.parse(o, false));
                                    }
                                }
                                case String s -> {
                                    if ((element.elementType() == ElementType.BYTE || element.elementType() == ElementType.BIT)
                                        && s.length() == dims * 2
                                        && ByteElement.isMaybeHexString(s)) {
                                        byte[] bytes;
                                        try {
                                            bytes = HexFormat.of().parseHex(s);
                                        } catch (IllegalArgumentException e) {
                                            bytes = Base64.getDecoder().decode(s);
                                        }
                                        for (byte b : bytes) {
                                            values.add((float) b);
                                        }
                                    } else {
                                        byte[] floatBytes = Base64.getDecoder().decode(s);
                                        float[] floats = new float[dims];
                                        ByteBuffer.wrap(floatBytes).asFloatBuffer().get(floats);
                                        for (float f : floats) {
                                            values.add(f);
                                        }
                                    }
                                }
                                default -> ignoredValues.add(sourceValue);
                            }
                        } catch (Exception e) {
                            // if parsing fails here then it would have failed at index time
                            // as well, meaning that we must be ignoring malformed values.
                            ignoredValues.add(sourceValue);
                        }
                    }
                    values.trimToSize();
                    return values;
                }

                @Override
                protected Object parseSourceValue(Object value) {
                    throw new IllegalStateException("parsing dense vector from source is not supported here");
                }
            };
        }
    }

    private final DenseVectorIndexOptions indexOptions;
    private final IndexVersion indexCreatedVersion;
    private final boolean isExcludeSourceVectors;
    private final List<VectorsFormatProvider> extraVectorsFormatProviders;

    private DenseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams params,
        DenseVectorIndexOptions indexOptions,
        IndexVersion indexCreatedVersion,
        boolean isExcludeSourceVectorsFinal,
        List<VectorsFormatProvider> vectorsFormatProviders
    ) {
        super(simpleName, mappedFieldType, params);
        this.indexOptions = indexOptions;
        this.indexCreatedVersion = indexCreatedVersion;
        this.isExcludeSourceVectors = isExcludeSourceVectorsFinal;
        this.extraVectorsFormatProviders = vectorsFormatProviders;
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
            int dims = fieldType().element.parseDimensionCount(context);
            DenseVectorFieldMapper.Builder builder = (Builder) getMergeBuilder();
            builder.dimensions(dims);
            Mapper update = builder.build(context.createDynamicMapperBuilderContext());
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
        fieldType().element.parseKnnVectorAndIndex(context, this);
    }

    private void parseBinaryDocValuesVectorAndIndex(DocumentParserContext context) throws IOException {
        // encode array of floats as array of integers and store into buf
        // this code is here and not in the VectorEncoderDecoder so not to create extra arrays
        int dims = fieldType().dims;
        Element element = fieldType().element;
        int numBytes = indexCreatedVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)
            ? element.getNumBytes(dims) + MAGNITUDE_BYTES
            : element.getNumBytes(dims);

        ByteBuffer byteBuffer = element.createByteBuffer(indexCreatedVersion, numBytes);
        VectorData vectorData = element.parseKnnVector(context, dims, (i, b) -> {
            if (b) {
                checkDimensionMatches(i, context);
            } else {
                checkDimensionExceeded(i, context);
            }
        }, fieldType().similarity);
        vectorData.addToBuffer(element, byteBuffer);
        if (indexCreatedVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)) {
            // encode vector magnitude at the end
            double dotProduct = element.computeSquaredMagnitude(vectorData);
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
        return new Builder(leafName(), indexCreatedVersion, isExcludeSourceVectors, extraVectorsFormatProviders).init(this);
    }

    private static DenseVectorIndexOptions parseIndexOptions(String fieldName, Object propNode, IndexVersion indexVersion) {
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
        return parsedType.parseIndexOptions(fieldName, indexOptionsMap, indexVersion);
    }

    /**
     * @return the custom kNN vectors format that is configured for this field or
     * {@code null} if the default format should be used.
     * @param defaultFormat      the default kNN vectors format to use if no custom format is configured
     * @param indexSettings      the index settings
     * @param threadPool         the thread pool to use for merging, or {@code null} if not available
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(
        KnnVectorsFormat defaultFormat,
        IndexSettings indexSettings,
        @Nullable ThreadPool threadPool
    ) {
        ExecutorService mergingExecutorService = null;
        int maxMergingWorkers = 1;
        if (indexSettings.isIntraMergeParallelismEnabled() && threadPool != null) {
            maxMergingWorkers = threadPool.info(ThreadPool.Names.MERGE).getMax();
            if (maxMergingWorkers > 1) {
                mergingExecutorService = threadPool.executor(ThreadPool.Names.MERGE);
            }
        }
        final KnnVectorsFormat format;
        ElementType elementType = fieldType().element.elementType();
        if (indexOptions == null) {
            format = switch (elementType) {
                case BYTE, FLOAT -> defaultFormat;
                case BIT, BFLOAT16 -> new ES93HnswVectorsFormat(
                    DEFAULT_MAX_CONN,
                    DEFAULT_MAX_CONN,
                    elementType,
                    maxMergingWorkers,
                    mergingExecutorService
                );
            };
        } else {
            // if plugins provided alternative KnnVectorsFormat for this indexOptions, use it instead of standard
            KnnVectorsFormat extraKnnFormat = null;
            for (VectorsFormatProvider vectorsFormatProvider : extraVectorsFormatProviders) {
                extraKnnFormat = vectorsFormatProvider.getKnnVectorsFormat(
                    indexSettings,
                    indexOptions,
                    fieldType().similarity(),
                    elementType,
                    mergingExecutorService,
                    maxMergingWorkers
                );
                if (extraKnnFormat != null) {
                    break;
                }
            }
            format = extraKnnFormat != null
                ? extraKnnFormat
                : indexOptions.getVectorsFormat(elementType, mergingExecutorService, maxMergingWorkers);
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
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader() {
        if (isExcludeSourceVectors) {
            return new SyntheticVectorsPatchFieldLoader<>(
                // Recreate the object for each leaf so that different segments can be searched concurrently.
                () -> new IndexedSyntheticFieldLoader(indexCreatedVersion, fieldType().similarity),
                IndexedSyntheticFieldLoader::copyVectorAsList
            );
        }
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(
            () -> fieldType().indexed
                ? new IndexedSyntheticFieldLoader(indexCreatedVersion, fieldType().similarity)
                : new DocValuesSyntheticFieldLoader(indexCreatedVersion)
        );
    }

    private class IndexedSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private FloatVectorValues floatValues;
        private ByteVectorValues byteValues;
        private NumericDocValues magnitudeReader;

        private boolean hasValue;
        private boolean hasMagnitude;
        private int ord;

        private final IndexVersion indexCreatedVersion;
        private final VectorSimilarity vectorSimilarity;
        private final Thread creationThread;

        private IndexedSyntheticFieldLoader(IndexVersion indexCreatedVersion, VectorSimilarity vectorSimilarity) {
            this.indexCreatedVersion = indexCreatedVersion;
            this.vectorSimilarity = vectorSimilarity;
            this.creationThread = Thread.currentThread();
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            floatValues = reader.getFloatVectorValues(fullPath());
            if (floatValues != null) {
                if (shouldNormalize()) {
                    magnitudeReader = reader.getNumericDocValues(fullPath() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                }
                return createLoader(floatValues.iterator(), true);
            }

            byteValues = reader.getByteVectorValues(fullPath());
            if (byteValues != null) {
                return createLoader(byteValues.iterator(), false);
            }

            return null;
        }

        private boolean shouldNormalize() {
            return indexCreatedVersion.onOrAfter(NORMALIZE_COSINE) && VectorSimilarity.COSINE.equals(vectorSimilarity);
        }

        private DocValuesLoader createLoader(KnnVectorValues.DocIndexIterator iterator, boolean checkMagnitude) {
            return docId -> {
                assert creationThread == Thread.currentThread()
                    : "Thread mismatch: created by [" + creationThread + "], but accessed by [" + Thread.currentThread() + "]";
                if (iterator.docID() > docId) {
                    return hasValue = false;
                }
                if (iterator.docID() == docId || iterator.advance(docId) == docId) {
                    ord = iterator.index();
                    hasValue = true;
                    hasMagnitude = checkMagnitude && magnitudeReader != null && magnitudeReader.advanceExact(docId);
                } else {
                    hasValue = false;
                }
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
            float magnitude = hasMagnitude ? Float.intBitsToFloat((int) magnitudeReader.longValue()) : Float.NaN;
            b.startArray(leafName());
            if (floatValues != null) {
                for (float v : floatValues.vectorValue(ord)) {
                    b.value(hasMagnitude ? v * magnitude : v);
                }
            } else if (byteValues != null) {
                for (byte v : byteValues.vectorValue(ord)) {
                    b.value(v);
                }
            }
            b.endArray();
        }

        /**
         * Returns a deep-copied vector for the current document, either as a list of floats
         * (with optional cosine normalization) or a list of bytes.
         *
         * @throws IOException if reading fails
         */
        private List<?> copyVectorAsList() throws IOException {
            assert hasValue : "vector is null for ord=" + ord;
            if (floatValues != null) {
                float[] raw = floatValues.vectorValue(ord);
                List<Float> copyList = new ArrayList<>(raw.length);

                if (hasMagnitude) {
                    float mag = Float.intBitsToFloat((int) magnitudeReader.longValue());
                    for (int i = 0; i < raw.length; i++) {
                        copyList.add(raw[i] * mag);
                    }
                } else {
                    for (int i = 0; i < raw.length; i++) {
                        copyList.add(raw[i]);
                    }
                }
                return copyList;
            } else if (byteValues != null) {
                byte[] raw = byteValues.vectorValue(ord);
                List<Byte> copyList = new ArrayList<>(raw.length);
                for (int i = 0; i < raw.length; i++) {
                    copyList.add(raw[i]);
                }
                return copyList;
            }

            throw new IllegalStateException("No vector values available to copy.");
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
                if (values.docID() > docId) {
                    return hasValue = false;
                }
                if (values.docID() == docId) {
                    return hasValue = true;
                }
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
            int dims = fieldType().element.elementType() == ElementType.BIT ? fieldType().dims / Byte.SIZE : fieldType().dims;
            for (int dim = 0; dim < dims; dim++) {
                fieldType().element.readAndWriteValue(byteBuffer, b);
            }
            b.endArray();
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }

    /**
     * Interface for a function that takes a int and boolean
     */
    @FunctionalInterface
    public interface IntBooleanConsumer {
        void accept(int value, boolean isComplete);
    }

    public interface SimilarityFunction {
        float calculateSimilarity(float[] leftVector, float[] rightVector);

        float calculateSimilarity(byte[] leftVector, byte[] rightVector);

        BlockLoaderFunctionConfig.Function function();
    }

    /**
     * Configuration for a {@link BlockLoaderFunctionConfig} that calculates vector similarity.
     * Functions that use this config should use SIMILARITY_FUNCTION_NAME as their name.
     */
    public static class VectorSimilarityFunctionConfig implements BlockLoaderFunctionConfig {

        private final SimilarityFunction similarityFunction;
        private final float[] vector;
        private final byte[] vectorAsBytes;

        public VectorSimilarityFunctionConfig(SimilarityFunction similarityFunction, float[] vector) {
            Objects.requireNonNull(vector);
            assert vector.length > 0 : "vector length must be > 0";
            this.similarityFunction = similarityFunction;
            this.vector = vector;
            this.vectorAsBytes = null;
        }

        public VectorSimilarityFunctionConfig(SimilarityFunction similarityFunction, byte[] vectorAsBytes) {
            this.similarityFunction = similarityFunction;
            this.vector = null;
            this.vectorAsBytes = vectorAsBytes;
        }

        @Override
        public Function function() {
            return similarityFunction.function();
        }

        public byte[] vectorAsBytes() {
            assert vectorAsBytes != null : "vectorAsBytes is null, maybe incorrect element type during construction?";
            return vectorAsBytes;
        }

        public float[] vector() {
            assert vector != null : "vector is null, maybe incorrect element type during construction?";
            return vector;
        }

        public SimilarityFunction similarityFunction() {
            return similarityFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            VectorSimilarityFunctionConfig that = (VectorSimilarityFunctionConfig) o;
            return Objects.equals(similarityFunction, that.similarityFunction)
                && Objects.deepEquals(vector, that.vector)
                && Objects.deepEquals(vectorAsBytes, that.vectorAsBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(similarityFunction, Arrays.hashCode(vector), Arrays.hashCode(vectorAsBytes));
        }
    }
}
