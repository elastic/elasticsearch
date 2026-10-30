/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ArraySourceValueFetcher;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorAutoCalibrate;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.Element;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.KnnVectorsFormatMapper;
import org.elasticsearch.index.mapper.vectors.SyntheticVectorsPatchFieldLoader;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT_BIT;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.namesToElementType;
import static org.elasticsearch.xpack.rank.vectors.RankVectorsFeatures.RANK_VECTORS_MUVERA_INDEXING;
import static org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin.RANK_VECTORS_FEATURE;

public class RankVectorsFieldMapper extends FieldMapper implements KnnVectorsFormatMapper {

    public static final String VECTOR_MAGNITUDES_SUFFIX = "._magnitude";
    public static final String CONTENT_TYPE = "rank_vectors";
    public static final int MAX_MUVERA_VECTORS = 50;

    static final int MUVERA_ENCODING_VERSION = 1;
    static final String MUVERA_ENCODING_PROFILE = "muvera_v1";
    static final int MUVERA_TOKEN_DIMS = 128;
    static final int MUVERA_FDE_DIMS = 4096;
    static final int MUVERA_K_SIM = 3;
    static final int MUVERA_D_PROJ = 8;
    static final int MUVERA_R_REPS = 64;
    static final long MUVERA_SEED = 42L;
    private static final int MUVERA_VECTORS_PER_CLUSTER = 384;
    private static final String MUVERA_INDEX_OPTIONS_TYPE = "muvera";
    private static final MuveraEncoder MUVERA_ENCODER = new MuveraEncoder(
        MUVERA_TOKEN_DIMS,
        MUVERA_K_SIM,
        MUVERA_D_PROJ,
        MUVERA_R_REPS,
        MUVERA_SEED
    );
    private static final Set<String> MUVERA_INDEX_OPTION_FIELDS = Set.of(
        "type",
        "encoding_version",
        "encoding_profile",
        "k_sim",
        "d_proj",
        "r_reps",
        "seed",
        "output_dims"
    );

    record MuveraIndexOptions(int encodingVersion, String encodingProfile, int kSim, int dProj, int rReps, long seed, int outputDims) {}

    /**
     * Maximum number of vectors accepted in a single rank vectors value, whether it is a document's field value or a
     * max-sim query vector. Some late-interaction models emit up to this many vectors for one piece of text:
     * Jina-ColBERT-v2, for instance, produces one vector per token over an 8192 token context.
     */
    public static final int MAX_VECTORS = 8192;

    private static RankVectorsFieldMapper toType(FieldMapper in) {
        return (RankVectorsFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<DenseVectorFieldMapper.ElementType> elementType = new Parameter<>(
            "element_type",
            false,
            () -> DenseVectorFieldMapper.ElementType.FLOAT,
            (n, c, o) -> {
                DenseVectorFieldMapper.ElementType elementType = namesToElementType.get((String) o);
                if (elementType == null) {
                    throw new MapperParsingException(
                        "invalid element_type [" + o + "]; available types are " + namesToElementType.keySet()
                    );
                }
                return elementType;
            },
            m -> toType(m).fieldType().element.elementType(),
            XContentBuilder::field,
            Objects::toString
        );

        // This is defined as updatable because it can be updated once, from [null] to a valid dim size,
        // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
        private final Parameter<Integer> dims = new Parameter<>("dims", true, () -> null, (n, c, o) -> {
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
                int maxDims = elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT ? MAX_DIMS_COUNT_BIT : MAX_DIMS_COUNT;
                int minDims = elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT ? Byte.SIZE : 1;
                if (dims < minDims || dims > maxDims) {
                    throw new MapperParsingException(
                        "The number of dimensions should be in the range [" + minDims + ", " + maxDims + "] but was [" + dims + "]"
                    );
                }
                if (elementType.getValue() == DenseVectorFieldMapper.ElementType.BIT) {
                    if (dims % Byte.SIZE != 0) {
                        throw new MapperParsingException("The number of dimensions for should be a multiple of 8 but was [" + dims + "]");
                    }
                }
            });
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).fieldType().indexed, false);
        private final Parameter<MuveraIndexOptions> indexOptions = new Parameter<>(
            "index_options",
            false,
            () -> null,
            (n, c, o) -> parseIndexOptions(n, o, c),
            m -> toType(m).fieldType().indexOptions,
            (b, n, v) -> {
                b.startObject(n);
                b.field("type", MUVERA_INDEX_OPTIONS_TYPE);
                b.field("encoding_version", v.encodingVersion());
                b.field("encoding_profile", v.encodingProfile());
                b.field("k_sim", v.kSim());
                b.field("d_proj", v.dProj());
                b.field("r_reps", v.rReps());
                b.field("seed", v.seed());
                b.field("output_dims", v.outputDims());
                b.endObject();
            },
            Objects::toString
        ).setSerializerCheck((id, ic, v) -> v != null)
            .setMergeValidator((previous, current, conflicts) -> Objects.equals(previous, current));
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final IndexVersion indexCreatedVersion;
        private final IndexMode indexMode;
        private final XPackLicenseState licenseState;
        private final boolean isExcludeSourceVectors;
        private final boolean experimentalFeaturesEnabled;
        private final List<VectorsFormatProvider> vectorsFormatProviders;

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexMode indexMode,
            XPackLicenseState licenseState,
            boolean isExcludeSourceVectors,
            boolean experimentalFeaturesEnabled,
            List<VectorsFormatProvider> vectorsFormatProviders
        ) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.indexMode = indexMode;
            this.licenseState = licenseState;
            this.isExcludeSourceVectors = isExcludeSourceVectors;
            this.experimentalFeaturesEnabled = experimentalFeaturesEnabled;
            this.vectorsFormatProviders = vectorsFormatProviders;
        }

        public Builder dimensions(int dimensions) {
            this.dims.setValue(dimensions);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { elementType, dims, indexed, indexOptions, meta };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public RankVectorsFieldMapper build(MapperBuilderContext context) {
            // Validate on Mapping creation
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            // Validate again here because the dimensions or element type could have been set programmatically,
            // which affects index option validity
            validate();
            validateMuveraConfiguration();
            boolean isExcludeSourceVectorsFinal = isExcludeSourceVectors && (context.isSourceStored() || context.isSourceColumnarStored());
            return new RankVectorsFieldMapper(
                leafName(),
                new RankVectorsFieldType(
                    context.buildFullName(leafName()),
                    elementType.getValue(),
                    dims.getValue(),
                    indexed.getValue(),
                    indexOptions.getValue(),
                    licenseState,
                    meta.getValue()
                ),
                builderParams(this, context),
                indexCreatedVersion,
                indexMode,
                licenseState,
                isExcludeSourceVectorsFinal,
                experimentalFeaturesEnabled,
                vectorsFormatProviders,
                indexed.getValue() ? MUVERA_ENCODER : null
            );
        }

        private void validateMuveraConfiguration() {
            if (indexed.getValue() == false) {
                if (indexOptions.getValue() != null) {
                    throw new IllegalArgumentException("[index_options] requires [index] to be [true]");
                }
                return;
            }
            if (indexOptions.getValue() == null) {
                throw new IllegalArgumentException(
                    "Field [index] set to [true] requires [index_options.type] to be [" + MUVERA_INDEX_OPTIONS_TYPE + "]"
                );
            }
            if (elementType.getValue() != ElementType.FLOAT) {
                throw new IllegalArgumentException(
                    "Indexed MUVERA rank_vectors fields require [element_type] to be [float], got [" + elementType.getValue() + "]"
                );
            }
            if (dims.getValue() == null) {
                throw new IllegalArgumentException("Indexed MUVERA rank_vectors fields require [dims] to be configured");
            }
            if (dims.getValue() != MUVERA_TOKEN_DIMS) {
                throw new IllegalArgumentException(
                    "MUVERA encoding version ["
                        + MUVERA_ENCODING_VERSION
                        + "] requires [dims] to be ["
                        + MUVERA_TOKEN_DIMS
                        + "], got ["
                        + dims.getValue()
                        + "]"
                );
            }
            if (vectorsFormatProviders == null
                || vectorsFormatProviders.stream()
                    .noneMatch(
                        provider -> provider.isVectorIndexTypeAllowed(indexCreatedVersion, DenseVectorFieldMapper.VectorIndexType.BBQ_DISK)
                    )) {
                throw new IllegalArgumentException("Indexed MUVERA rank_vectors fields require the [bbq_disk] vector format provider");
            }
        }

        private static MuveraIndexOptions parseIndexOptions(String fieldName, Object node, MappingParserContext context) {
            if (context.clusterHasFeature(RANK_VECTORS_MUVERA_INDEXING) == false) {
                throw new MapperParsingException(
                    "Cannot create indexed MUVERA rank_vectors field ["
                        + fieldName
                        + "] until all nodes support feature ["
                        + RANK_VECTORS_MUVERA_INDEXING.id()
                        + "]"
                );
            }
            Map<String, Object> options = XContentMapValues.nodeMapValue(node, "index_options");
            if (MUVERA_INDEX_OPTIONS_TYPE.equals(options.get("type")) == false) {
                throw new MapperParsingException(
                    "[index_options.type] on field [" + fieldName + "] must be [" + MUVERA_INDEX_OPTIONS_TYPE + "]"
                );
            }
            Set<String> unknownFields = new java.util.HashSet<>(options.keySet());
            unknownFields.removeAll(MUVERA_INDEX_OPTION_FIELDS);
            if (unknownFields.isEmpty() == false) {
                throw new MapperParsingException("Unknown MUVERA index option fields " + unknownFields + " on field [" + fieldName + "]");
            }
            if (options.containsKey("encoding_version") == false || options.containsKey("encoding_profile") == false) {
                throw new MapperParsingException(
                    "MUVERA [index_options] on field [" + fieldName + "] require explicit [encoding_version] and [encoding_profile]"
                );
            }
            int encodingVersion = integerOption(options, "encoding_version", MUVERA_ENCODING_VERSION);
            String encodingProfile = Objects.toString(options.get("encoding_profile"));
            int kSim = integerOption(options, "k_sim", MUVERA_K_SIM);
            int dProj = integerOption(options, "d_proj", MUVERA_D_PROJ);
            int rReps = integerOption(options, "r_reps", MUVERA_R_REPS);
            long seed = longOption(options, "seed", MUVERA_SEED);
            int outputDims = integerOption(options, "output_dims", MUVERA_FDE_DIMS);
            if (encodingVersion != MUVERA_ENCODING_VERSION
                || encodingProfile.equals(MUVERA_ENCODING_PROFILE) == false
                || kSim != MUVERA_K_SIM
                || dProj != MUVERA_D_PROJ
                || rReps != MUVERA_R_REPS
                || seed != MUVERA_SEED
                || outputDims != MUVERA_FDE_DIMS) {
                throw new MapperParsingException(
                    "Unsupported MUVERA profile on field ["
                        + fieldName
                        + "]; supported encoding identity is [version="
                        + MUVERA_ENCODING_VERSION
                        + ", profile="
                        + MUVERA_ENCODING_PROFILE
                        + ", token_dims="
                        + MUVERA_TOKEN_DIMS
                        + ", k_sim="
                        + MUVERA_K_SIM
                        + ", d_proj="
                        + MUVERA_D_PROJ
                        + ", r_reps="
                        + MUVERA_R_REPS
                        + ", seed="
                        + MUVERA_SEED
                        + ", output_dims="
                        + MUVERA_FDE_DIMS
                        + "]"
                );
            }
            return new MuveraIndexOptions(encodingVersion, encodingProfile, kSim, dProj, rReps, seed, outputDims);
        }

        private static int integerOption(Map<String, Object> options, String name, int defaultValue) {
            Object value = options.get(name);
            if (value == null) {
                return defaultValue;
            }
            if (value instanceof Integer integer) {
                return integer;
            }
            throw new MapperParsingException("MUVERA index option [" + name + "] must be an integer");
        }

        private static long longOption(Map<String, Object> options, String name, long defaultValue) {
            Object value = options.get(name);
            if (value == null) {
                return defaultValue;
            }
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return ((Number) value).longValue();
            }
            throw new MapperParsingException("MUVERA index option [" + name + "] must be an integer in the signed 64-bit range");
        }
    }

    public static final class RankVectorsFieldType extends SimpleMappedFieldType {
        private final Element element;
        private final Integer dims;
        private final boolean indexed;
        private final MuveraIndexOptions indexOptions;
        private final XPackLicenseState licenseState;

        public RankVectorsFieldType(
            String name,
            ElementType elementType,
            Integer dims,
            XPackLicenseState licenseState,
            Map<String, String> meta
        ) {
            this(name, elementType, dims, false, null, licenseState, meta);
        }

        public RankVectorsFieldType(
            String name,
            ElementType elementType,
            Integer dims,
            boolean indexed,
            MuveraIndexOptions indexOptions,
            XPackLicenseState licenseState,
            Map<String, String> meta
        ) {
            super(name, indexed ? IndexType.vectorsWithDocValues() : IndexType.docValuesOnly(), false, meta);
            this.element = Element.getElement(elementType);
            this.dims = dims;
            this.indexed = indexed;
            this.indexOptions = indexOptions;
            this.licenseState = licenseState;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public boolean isVectorEmbedding() {
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new ArraySourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    List<?> outerList = (List<?>) value;
                    List<Object> vectors = new ArrayList<>(outerList.size());
                    for (Object o : outerList) {
                        if (o instanceof List<?> innerList) {
                            float[] vector = new float[innerList.size()];
                            for (int i = 0; i < vector.length; i++) {
                                vector[i] = ((Number) innerList.get(i)).floatValue();
                            }
                            vectors.add(vector);
                        } else {
                            vectors.add(o);
                        }
                    }
                    return vectors;
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.DENSE_VECTOR;
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new RankVectorsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, dims, element.elementType());
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support term queries");
        }

        public Query maxSimQuery(float[][] queryVectors, int k, int numCandidates, @Nullable Query filter) {
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            if (indexed == false || indexOptions == null) {
                throw new IllegalArgumentException("Field [" + name() + "] is not indexed for approximate MaxSim search");
            }
            return new MuveraMaxSimQuery(name(), queryVectors, k, numCandidates, filter);
        }

        /** Returns a native brute-force MaxSim query for evaluation baselines. */
        public Query exactMaxSimQuery(float[][] queryVectors) {
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            return new ExactRankVectorsQuery(name(), queryVectors, dims);
        }

        /** Returns the raw per-shard MUVERA candidate query used by the private evaluation action. */
        public Query muveraCandidateQuery(float[][] queryVectors, int numCandidates) {
            if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
                throw LicenseUtils.newComplianceException("Rank Vectors");
            }
            if (indexed == false || indexOptions == null) {
                throw new IllegalArgumentException("Field [" + name() + "] is not indexed with MUVERA");
            }
            return new MuveraCandidateQuery(name(), queryVectors, numCandidates);
        }

        public int getVectorDimensions() {
            return dims;
        }

        public DenseVectorFieldMapper.ElementType getElementType() {
            return element.elementType();
        }

        public boolean isMuveraIndexed() {
            return indexed && indexOptions != null;
        }

        public String getMuveraEncodingProfile() {
            return indexOptions == null ? null : indexOptions.encodingProfile();
        }

        public int getMuveraEncodingVersion() {
            return indexOptions == null ? -1 : indexOptions.encodingVersion();
        }
    }

    private final IndexVersion indexCreatedVersion;
    private final IndexMode indexMode;
    private final XPackLicenseState licenseState;
    private final boolean isExcludeSourceVectors;
    private final boolean experimentalFeaturesEnabled;
    private final List<VectorsFormatProvider> vectorsFormatProviders;
    @Nullable
    private final DenseVectorFieldMapper.DenseVectorIndexOptions muveraCandidateIndexOptions;
    private final MuveraEncoder muveraEncoder;

    static float[] encodeMuveraQuery(float[][] queryVectors) {
        return MUVERA_ENCODER.encodeQuery(queryVectors);
    }

    static float[] encodeMuveraQuery(float[][] queryVectors, Runnable checkCancelled) {
        return MUVERA_ENCODER.encodeQuery(queryVectors, checkCancelled);
    }

    private RankVectorsFieldMapper(
        String simpleName,
        MappedFieldType fieldType,
        BuilderParams params,
        IndexVersion indexCreatedVersion,
        IndexMode indexMode,
        XPackLicenseState licenseState,
        boolean isExcludeSourceVectors,
        boolean experimentalFeaturesEnabled,
        List<VectorsFormatProvider> vectorsFormatProviders,
        MuveraEncoder muveraEncoder
    ) {
        super(simpleName, fieldType, params);
        this.indexCreatedVersion = indexCreatedVersion;
        this.indexMode = indexMode;
        this.licenseState = licenseState;
        this.isExcludeSourceVectors = isExcludeSourceVectors;
        this.experimentalFeaturesEnabled = experimentalFeaturesEnabled;
        this.vectorsFormatProviders = vectorsFormatProviders;
        this.muveraEncoder = muveraEncoder;
        this.muveraCandidateIndexOptions = muveraEncoder == null ? null : createMuveraCandidateIndexOptions();
    }

    private DenseVectorFieldMapper.DenseVectorIndexOptions createMuveraCandidateIndexOptions() {
        return new DenseVectorFieldMapper.BBQIVFIndexOptions(
            MUVERA_VECTORS_PER_CLUSTER,
            -1,
            0d,
            false,
            new DenseVectorFieldMapper.RescoreVector(DenseVectorFieldMapper.DEFAULT_OVERSAMPLE),
            indexCreatedVersion,
            false,
            1,
            experimentalFeaturesEnabled,
            DenseVectorAutoCalibrate.disabled(),
            DenseVectorFieldMapper.BBQIVFIndexOptions.QuantizationType.OSQ,
            false
        );
    }

    @Override
    public RankVectorsFieldType fieldType() {
        return (RankVectorsFieldType) super.fieldType();
    }

    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    @Override
    public ParseResult parse(DocumentParserContext context) throws IOException {
        if (RANK_VECTORS_FEATURE.check(licenseState) == false) {
            throw LicenseUtils.newComplianceException("Rank Vectors");
        }
        if (context.doc().getByKey(fieldType().name()) != null) {
            throw new IllegalArgumentException(
                "Field ["
                    + fullPath()
                    + "] of type ["
                    + typeName()
                    + "] doesn't support indexing multiple values for the same field in the same document"
            );
        }
        if (XContentParser.Token.VALUE_NULL == context.parser().currentToken()) {
            return ParseResult.INDEXED;
        }
        if (XContentParser.Token.START_ARRAY != context.parser().currentToken()) {
            throw new IllegalArgumentException(
                "Field [" + fullPath() + "] of type [" + typeName() + "] cannot be indexed with a single value"
            );
        }
        if (fieldType().dims == null) {
            int currentDims = -1;
            int vectorCount = 0;
            while (XContentParser.Token.END_ARRAY != context.parser().nextToken()) {
                int dims = fieldType().element.parseDimensionCount(context);
                if (++vectorCount > MAX_VECTORS) {
                    throw tooManyVectors();
                }
                if (currentDims == -1) {
                    currentDims = dims;
                } else if (currentDims != dims) {
                    throw new IllegalArgumentException(
                        "Field [" + fullPath() + "] of type [" + typeName() + "] cannot be indexed with vectors of different dimensions"
                    );
                }
            }
            if (currentDims == -1) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fullPath()
                        + "] of type ["
                        + typeName()
                        + "] requires at least one vector; use null to indicate a missing value"
                );
            }
            var builder = (Builder) getMergeBuilder();
            builder.dimensions(currentDims);
            context.addDynamicMapper(builder, fullPath());
            return ParseResult.INDEXED;
        }
        int dims = fieldType().dims;
        Element element = fieldType().element;
        List<VectorData> vectors = new ArrayList<>();
        while (XContentParser.Token.END_ARRAY != context.parser().nextToken()) {
            VectorData vector = element.parseKnnVector(context, dims, (i, b) -> {
                if (b) {
                    checkDimensionMatches(i, context);
                } else {
                    checkDimensionExceeded(i, context);
                }
            }, null);
            vectors.add(vector);
            if (vectors.size() > maxVectors()) {
                throw tooManyVectors();
            }
        }
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException(
                "Field [" + fullPath() + "] of type [" + typeName() + "] requires at least one vector; use null to indicate a missing value"
            );
        }
        int bufferSize = element.getNumBytes(dims) * vectors.size();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer magnitudeBuffer = ByteBuffer.allocate(vectors.size() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (VectorData vector : vectors) {
            vector.addToBuffer(element, buffer);
            magnitudeBuffer.putFloat((float) Math.sqrt(element.computeSquaredMagnitude(vector)));
        }
        String vectorFieldName = fieldType().name();
        String vectorMagnitudeFieldName = vectorFieldName + VECTOR_MAGNITUDES_SUFFIX;
        context.doc().addWithKey(vectorFieldName, new BinaryDocValuesField(vectorFieldName, new BytesRef(buffer.array())));
        context.doc()
            .addWithKey(
                vectorMagnitudeFieldName,
                new BinaryDocValuesField(vectorMagnitudeFieldName, new BytesRef(magnitudeBuffer.array()))
            );
        if (muveraEncoder != null) {
            float[][] tokenVectors = new float[vectors.size()][];
            for (int i = 0; i < vectors.size(); i++) {
                tokenVectors[i] = vectors.get(i).asFloatVector();
            }
            context.doc()
                .add(
                    new KnnFloatVectorField(
                        vectorFieldName,
                        muveraEncoder.encodeDocument(tokenVectors),
                        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
                    )
                );
        }
        return ParseResult.INDEXED;
    }

    private int maxVectors() {
        return muveraEncoder == null ? MAX_VECTORS : MAX_MUVERA_VECTORS;
    }

    private IllegalArgumentException tooManyVectors() {
        return new IllegalArgumentException(
            "Field ["
                + fullPath()
                + "] of type ["
                + typeName()
                + "] cannot be indexed with more than ["
                + maxVectors()
                + "] vectors in a single document"
        );
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
        return new Builder(
            leafName(),
            indexCreatedVersion,
            indexMode,
            licenseState,
            isExcludeSourceVectors,
            experimentalFeaturesEnabled,
            vectorsFormatProviders
        ).init(this);
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(
        KnnVectorsFormat defaultFormat,
        IndexSettings indexSettings,
        @Nullable ThreadPool threadPool
    ) {
        if (muveraCandidateIndexOptions == null) {
            return defaultFormat;
        }
        ExecutorService mergingExecutorService = null;
        int maxMergingWorkers = 1;
        if (indexSettings.isIntraMergeParallelismEnabled() && threadPool != null && threadPool.info(ThreadPool.Names.MERGE) != null) {
            maxMergingWorkers = threadPool.info(ThreadPool.Names.MERGE).getMax();
            if (maxMergingWorkers > 1) {
                mergingExecutorService = threadPool.executor(ThreadPool.Names.MERGE);
            }
        }
        ExecutorService quantizerExecutorService = threadPool == null ? null : threadPool.executor(ThreadPool.Names.MERGE);
        KnnVectorsFormat selected = null;
        for (VectorsFormatProvider provider : vectorsFormatProviders) {
            selected = provider.getKnnVectorsFormat(
                indexSettings,
                muveraCandidateIndexOptions,
                DenseVectorFieldMapper.VectorSimilarity.MAX_INNER_PRODUCT,
                ElementType.FLOAT,
                mergingExecutorService,
                maxMergingWorkers,
                quantizerExecutorService
            );
            if (selected != null) {
                break;
            }
        }
        if (selected == null) {
            throw new IllegalStateException(
                "Indexed MUVERA rank_vectors field [" + fullPath() + "] requires the [bbq_disk] vector format provider"
            );
        }
        KnnVectorsFormat format = selected;
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
                return MUVERA_FDE_DIMS;
            }

            @Override
            public String toString() {
                return format.toString();
            }
        };
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(DocValuesSyntheticFieldLoader::new);
    }

    @Override
    public SourceLoader.SyntheticVectorsLoader syntheticVectorsLoader() {
        if (isExcludeSourceVectors) {
            return new SyntheticVectorsPatchFieldLoader<>(
                // Recreate the object for each leaf so that different segments can be searched concurrently.
                DocValuesSyntheticFieldLoader::new,
                DocValuesSyntheticFieldLoader::copyVectorsAsList
            );
        }
        return null;
    }

    private class DocValuesSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
        private BinaryDocValues values;
        private boolean hasValue;

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
                return hasValue = values.docID() == docId || values.advance(docId) == docId;
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
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length).order(ByteOrder.LITTLE_ENDIAN);
            assert ref.length % fieldType().element.getNumBytes(fieldType().dims) == 0;
            int numVecs = ref.length / fieldType().element.getNumBytes(fieldType().dims);
            int vectorLength = fieldType().element.elementType().vectorLength(fieldType().dims);
            for (int i = 0; i < numVecs; i++) {
                b.startArray();
                for (int j = 0; j < vectorLength; j++) {
                    fieldType().element.readAndWriteValue(byteBuffer, b);
                }
                b.endArray();
            }
            b.endArray();
        }

        /**
         * Returns deep-copied vectors  for the current document, either as a list.
         *
         * @throws IOException if reading fails
         */
        private List<List<?>> copyVectorsAsList() throws IOException {
            assert hasValue : "rank vector is null";
            BytesRef ref = values.binaryValue();
            ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length).order(ByteOrder.LITTLE_ENDIAN);
            assert ref.length % fieldType().element.getNumBytes(fieldType().dims) == 0;
            int numVecs = ref.length / fieldType().element.getNumBytes(fieldType().dims);
            List<List<?>> vectors = new ArrayList<>(numVecs);
            int vectorLength = fieldType().element.elementType().vectorLength(fieldType().dims);
            for (int i = 0; i < numVecs; i++) {
                switch (fieldType().element.elementType()) {
                    case FLOAT -> {
                        List<Float> vec = new ArrayList<>(vectorLength);
                        for (int j = 0; j < vectorLength; j++) {
                            vec.add(byteBuffer.getFloat());
                        }
                        vectors.add(vec);
                    }
                    case BFLOAT16 -> {
                        List<Float> vec = new ArrayList<>(vectorLength);
                        for (int j = 0; j < vectorLength; j++) {
                            vec.add(BFloat16.bFloat16ToFloat(byteBuffer.getShort()));
                        }
                        vectors.add(vec);
                    }
                    case BYTE, BIT -> {
                        List<Byte> vec = new ArrayList<>(vectorLength);
                        for (int j = 0; j < vectorLength; j++) {
                            vec.add(byteBuffer.get());
                        }
                        vectors.add(vec);
                    }
                }
            }
            return vectors;
        }

        @Override
        public String fieldName() {
            return fullPath();
        }
    }
}
