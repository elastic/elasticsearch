/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexVersions.NEW_SPARSE_VECTOR;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_USES_DENSE_VECTOR_DEFAULT_INDEX_OPTIONS;
import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_OFFSET_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOriginalTextFieldName;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_JINA_V5_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_ELSER_ID;

/**
 * A {@link FieldMapper} for semantic text fields.
 */
public class SemanticTextFieldMapper extends SemanticFieldMapper {
    private static final Logger logger = LogManager.getLogger(SemanticTextFieldMapper.class);

    public static final NodeFeature SEMANTIC_TEXT_IN_OBJECT_FIELD_FIX = new NodeFeature("semantic_text.in_object_field_fix");
    public static final NodeFeature SEMANTIC_TEXT_SINGLE_FIELD_UPDATE_FIX = new NodeFeature("semantic_text.single_field_update_fix");
    public static final NodeFeature SEMANTIC_TEXT_DELETE_FIX = new NodeFeature("semantic_text.delete_fix");
    public static final NodeFeature SEMANTIC_TEXT_ZERO_SIZE_FIX = new NodeFeature("semantic_text.zero_size_fix");
    public static final NodeFeature SEMANTIC_TEXT_ALWAYS_EMIT_INFERENCE_ID_FIX = new NodeFeature(
        "semantic_text.always_emit_inference_id_fix"
    );
    public static final NodeFeature SEMANTIC_TEXT_HANDLE_EMPTY_INPUT = new NodeFeature("semantic_text.handle_empty_input");
    public static final NodeFeature SEMANTIC_TEXT_SKIP_INFERENCE_FIELDS = new NodeFeature("semantic_text.skip_inference_fields");
    public static final NodeFeature SEMANTIC_TEXT_BIT_VECTOR_SUPPORT = new NodeFeature("semantic_text.bit_vector_support");
    public static final NodeFeature SEMANTIC_TEXT_SUPPORT_CHUNKING_CONFIG = new NodeFeature("semantic_text.support_chunking_config");
    public static final NodeFeature SEMANTIC_TEXT_EXCLUDE_SUB_FIELDS_FROM_FIELD_CAPS = new NodeFeature(
        "semantic_text.exclude_sub_fields_from_field_caps"
    );
    public static final NodeFeature SEMANTIC_TEXT_INDEX_OPTIONS = new NodeFeature("semantic_text.index_options");
    public static final NodeFeature SEMANTIC_TEXT_INDEX_OPTIONS_WITH_DEFAULTS = new NodeFeature(
        "semantic_text.index_options_with_defaults"
    );
    public static final NodeFeature SEMANTIC_TEXT_SPARSE_VECTOR_INDEX_OPTIONS = new NodeFeature(
        "semantic_text.sparse_vector_index_options"
    );
    public static final NodeFeature SEMANTIC_TEXT_UPDATABLE_INFERENCE_ID = new NodeFeature("semantic_text.updatable_inference_id");
    public static final NodeFeature SEMANTIC_TEXT_AUTO_PREFILTERING = new NodeFeature("semantic_text.auto_prefiltering");
    public static final NodeFeature SEMANTIC_TEXT_BFLOAT16_SUPPORT = new NodeFeature("semantic_text.bfloat16_support");
    public static final NodeFeature SEMANTIC_TEXT_ELEMENT_TYPE_IN_INDEX_OPTIONS = new NodeFeature(
        "semantic_text.element_type_in_index_options"
    );
    public static final NodeFeature SEMANTIC_TEXT_PREVENT_LEGACY_FORMAT_NEW_INDICES = new NodeFeature(
        "semantic_text.prevent_legacy_format_new_indices"
    );

    public static final String CONTENT_TYPE = "semantic_text";
    public static final String DEFAULT_FALLBACK_ELSER_INFERENCE_ID = DEFAULT_ELSER_ID;
    public static final String DEFAULT_EIS_ELSER_INFERENCE_ID = DEFAULT_ELSER_ENDPOINT_ID_V2;
    public static final String DEFAULT_EIS_JINA_V5_INFERENCE_ID = DEFAULT_JINA_V5_ENDPOINT_ID;

    public static final float DEFAULT_RESCORE_OVERSAMPLE = 3.0f;

    private static final DenseVectorMapperConfigurator DENSE_VECTOR_MAPPER_CONFIGURATOR = new DenseVectorMapperConfigurator(
        (indexVersion, modelElementType) -> defaultElementTypeToBfloat16(indexVersion, modelElementType)
            ? DenseVectorFieldMapper.ElementType.BFLOAT16
            : modelElementType,
        (indexVersion, modelSimilarity) -> {
            // Skip setting similarity on pre 8.11 indices. It causes dense vector field creation to fail because similarity can only be set
            // on indexed fields, which is not done by default prior to 8.11. The fact that the dense vector field is partially configured
            // is moot because we will explicitly fail to index docs into this semantic text field anyways.
            return indexVersion.onOrAfter(NEW_SPARSE_VECTOR) && modelSimilarity != null ? modelSimilarity.vectorSimilarity() : null;
        },
        SemanticTextFieldMapper::defaultDenseVectorIndexOptions
    );

    /**
     * An index setting that allows users to pin the default inference ID for {@code semantic_text} fields that do not declare an explicit
     * {@code inference_id}. Setting this in an index template insulates users from cluster-level default changes in the inference id.
     * <p>
     * The value is not validated against existing inference endpoints at index creation time; an invalid ID will only surface as an error
     * when a document is indexed against a {@code semantic_text} field that uses this default.
     */
    public static final Setting<String> INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID = Setting.simpleString(
        "index.semantic_text.default_inference_id",
        new Setting.Validator<>() {
            @Override
            public void validate(String value) {}

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent && Strings.isNullOrBlank(value)) {
                    throw new IllegalArgumentException("[index.semantic_text.default_inference_id] must not be blank");
                }
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.ServerlessPublic
    );

    public static final String UNSUPPORTED_INDEX_MESSAGE = "["
        + CONTENT_TYPE
        + "] is available on indices created with 8.11 or higher. Please create a new index to use ["
        + CONTENT_TYPE
        + "]";

    /**
     * Determines the default inference ID for {@code semantic_text} fields that do not declare an explicit {@code inference_id}.
     * <p>
     * Resolution order:
     * <ol>
     *   <li>If {@link #INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID} is set on the index, that value is returned directly.</li>
     *   <li>For indices created on or after {@code SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5}, if the model registry is non-null and
     *       {@link #DEFAULT_EIS_JINA_V5_INFERENCE_ID} is a registered preconfigured endpoint, that endpoint is returned.</li>
     *   <li>If the model registry is non-null and {@link #DEFAULT_EIS_ELSER_INFERENCE_ID} is a registered preconfigured endpoint,
     *       that endpoint is returned.</li>
     *   <li>Otherwise, falls back to {@link #DEFAULT_FALLBACK_ELSER_INFERENCE_ID} (ML-node ELSER).</li>
     * </ol>
     */
    private static String getDefaultInferenceId(ModelRegistry modelRegistry, IndexSettings indexSettings) {
        if (INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.exists(indexSettings.getSettings())) {
            return INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.get(indexSettings.getSettings());
        }
        if (modelRegistry != null) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5)
                && modelRegistry.containsPreconfiguredInferenceEndpointId(DEFAULT_EIS_JINA_V5_INFERENCE_ID)) {
                return DEFAULT_EIS_JINA_V5_INFERENCE_ID;
            }
            if (modelRegistry.containsPreconfiguredInferenceEndpointId(DEFAULT_EIS_ELSER_INFERENCE_ID)) {
                return DEFAULT_EIS_ELSER_INFERENCE_ID;
            }
        }
        return DEFAULT_FALLBACK_ELSER_INFERENCE_ID;
    }

    public static TypeParser parser(Supplier<ModelRegistry> modelRegistry) {
        return new TypeParser(
            (n, c) -> new Builder(n, c::bitSetProducer, c.getIndexSettings(), modelRegistry.get(), c.getVectorsFormatProviders()),
            List.of(validateParserContext(CONTENT_TYPE))
        );
    }

    public static BiConsumer<String, MappingParserContext> validateParserContext(String type) {
        return (n, c) -> {
            if (useLegacyFormat(c.getIndexSettings())) {
                notInMultiFields(type).accept(n, c);
            }
            notFromDynamicTemplates(type).accept(n, c);
        };
    }

    public static class Builder extends SemanticFieldMapper.Builder {
        private final boolean useLegacyFormat;

        public Builder(
            String name,
            Function<Query, BitSetProducer> bitSetProducer,
            IndexSettings indexSettings,
            ModelRegistry modelRegistry,
            List<VectorsFormatProvider> vectorsFormatProviders
        ) {
            super(name, bitSetProducer, indexSettings, modelRegistry, vectorsFormatProviders);
            this.useLegacyFormat = useLegacyFormat(indexSettings);
        }

        public Builder(SemanticTextFieldMapper mapper) {
            super(mapper);
            this.useLegacyFormat = useLegacyFormat(indexSettings);
        }

        @Override
        public Builder setInferenceId(String id) {
            return (SemanticTextFieldMapper.Builder) super.setInferenceId(id);
        }

        @Override
        public Builder setModelSettings(MinimalServiceSettings value) {
            return (SemanticTextFieldMapper.Builder) super.setModelSettings(value);
        }

        @Override
        public Builder setChunkingSettings(ChunkingSettings value) {
            return (SemanticTextFieldMapper.Builder) super.setChunkingSettings(value);
        }

        @Override
        protected String getDefaultInferenceId() {
            return SemanticTextFieldMapper.getDefaultInferenceId(modelRegistry, indexSettings);
        }

        @Override
        protected Parameter<SemanticTextIndexOptions> configureIndexOptionsParam() {
            return new Parameter<>(
                INDEX_OPTIONS_FIELD,
                true,
                () -> null,
                (n, c, o) -> parseIndexOptionsFromMap(n, o, c.indexVersionCreated(), experimentalFeaturesEnabled),
                mapper -> ((SemanticTextFieldType) mapper.fieldType()).indexOptions,
                (b, n, v) -> {
                    throw new IllegalStateException("Serializer for [" + INDEX_OPTIONS_FIELD + "] should not be called");
                },
                Objects::toString
            ) {
                @Override
                protected void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
                    SemanticTextIndexOptions value = getValue();
                    if (includeDefaults || isConfigured()) {
                        if (value == null) {
                            // Default value, serialize resolved defaults
                            MinimalServiceSettings resolvedModelSettings = getResolvedModelSettings(null, false);
                            value = defaultIndexOptions(indexVersionCreated, resolvedModelSettings);
                        } else if (value.type() == SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR) {
                            ExtendedDenseVectorIndexOptions innerIndexOptions = getExtendedDenseVectorIndexOptions(value);
                            DenseVectorFieldMapper.ElementType elementTypeOverride = innerIndexOptions.getElementType();
                            DenseVectorFieldMapper.DenseVectorIndexOptions dvio = innerIndexOptions.getBaseIndexOptions();

                            MinimalServiceSettings resolvedModelSettings = getResolvedModelSettings(null, false);
                            if (resolvedModelSettings == null) {
                                throw new IllegalStateException("Model settings should be resolvable when explicit index options are set");
                            }

                            if (defaultElementTypeToBfloat16(indexVersionCreated, resolvedModelSettings.elementType())
                                && includeDefaults
                                && elementTypeOverride == null) {
                                value = new SemanticTextIndexOptions(
                                    SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                                    new ExtendedDenseVectorIndexOptions(dvio, DenseVectorFieldMapper.ElementType.BFLOAT16)
                                );
                            }
                        }

                        builder.field(INDEX_OPTIONS_FIELD, value);
                    }
                }
            }.acceptsNull();
        }

        @Override
        protected NestedObjectMapper.Builder createChunksField(@Nullable MinimalServiceSettings resolvedModelSettings) {
            NestedObjectMapper.Builder chunksField = new NestedObjectMapper.Builder(
                SemanticTextField.CHUNKS_FIELD,
                indexVersionCreated,
                bitSetProducer,
                indexSettings
            );
            chunksField.dynamic(ObjectMapper.Dynamic.FALSE);
            if (resolvedModelSettings != null) {
                chunksField.add(createEmbeddingsField(resolvedModelSettings));
            }
            if (useLegacyFormat) {
                var chunkTextField = new KeywordFieldMapper.Builder(TEXT_FIELD, indexSettings).indexed(false).docValues(false);
                chunksField.add(chunkTextField);
            } else {
                chunksField.add(new OffsetSourceFieldMapper.Builder(CHUNKED_OFFSET_FIELD));
            }
            return chunksField;
        }

        @Override
        protected Mapper.Builder createEmbeddingsField(MinimalServiceSettings modelSettings) {
            return switch (modelSettings.taskType()) {
                case SPARSE_EMBEDDING -> {
                    SparseVectorFieldMapper.Builder sparseVectorMapperBuilder = new SparseVectorFieldMapper.Builder(
                        CHUNKED_EMBEDDINGS_FIELD,
                        indexVersionCreated,
                        false
                    ).setStored(useLegacyFormat == false);
                    configureSparseVectorMapperBuilder(indexVersionCreated, sparseVectorMapperBuilder, indexOptions.get());
                    yield sparseVectorMapperBuilder;
                }
                case TEXT_EMBEDDING, EMBEDDING -> {
                    DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                        CHUNKED_EMBEDDINGS_FIELD,
                        indexVersionCreated,
                        false,
                        experimentalFeaturesEnabled,
                        vectorsFormatProviders,
                        false
                    );
                    ExtendedDenseVectorIndexOptions extendedIndexOptions = indexOptions.get() != null
                        ? getExtendedDenseVectorIndexOptions(indexOptions.get())
                        : null;
                    DENSE_VECTOR_MAPPER_CONFIGURATOR.configure(
                        denseVectorMapperBuilder,
                        indexVersionCreated,
                        modelSettings,
                        extendedIndexOptions
                    );
                    yield denseVectorMapperBuilder;
                }
                default -> throw new IllegalArgumentException(
                    "Invalid task_type in model_settings [" + modelSettings.taskType().name() + "]"
                );
            };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            if (useLegacyFormat && copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            if (useLegacyFormat && multiFieldsBuilder.hasMultiFields()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support multi-fields");
            }

            return (SemanticTextFieldMapper) super.build(context);
        }

        @Override
        protected void validateTaskType(MinimalServiceSettings modelSettings) {
            switch (modelSettings.taskType()) {
                case SPARSE_EMBEDDING, TEXT_EMBEDDING, EMBEDDING -> {
                }
                default -> throw new IllegalArgumentException(
                    "Wrong ["
                        + MinimalServiceSettings.TASK_TYPE_FIELD
                        + "], expected "
                        + TEXT_EMBEDDING
                        + ", "
                        + EMBEDDING
                        + ", or "
                        + SPARSE_EMBEDDING
                        + ", got "
                        + modelSettings.taskType().name()
                );
            }
        }

        @Override
        protected void validateIndexOptions(MinimalServiceSettings modelSettings) {
            SemanticTextIndexOptions indexOptions = this.indexOptions.get();
            String inferenceId = this.inferenceId.get();

            if (indexOptions == null) {
                return;
            }

            if (modelSettings == null) {
                throw new IllegalArgumentException(
                    "Model settings must be set to validate index options for inference ID [" + inferenceId + "]"
                );
            }

            if (indexOptions.type() == SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR) {
                if (modelSettings.taskType() != SPARSE_EMBEDDING) {
                    throw new IllegalArgumentException(
                        "Invalid task type for index options, required ["
                            + SPARSE_EMBEDDING
                            + "] but was ["
                            + modelSettings.taskType()
                            + "]"
                    );
                }
                return;
            }

            if (indexOptions.type() == SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR) {
                if (modelSettings.taskType() != TEXT_EMBEDDING && modelSettings.taskType() != EMBEDDING) {
                    throw new IllegalArgumentException(
                        "Invalid task type for index options, required ["
                            + TEXT_EMBEDDING
                            + "] or ["
                            + EMBEDDING
                            + "] but was ["
                            + modelSettings.taskType()
                            + "]"
                    );
                }

                DenseVectorFieldMapper.ElementType elementType = modelSettings.elementType();
                ExtendedDenseVectorIndexOptions innerIndexOptions = getExtendedDenseVectorIndexOptions(indexOptions);
                if (innerIndexOptions.getElementType() != null) {
                    validateElementTypeOverride(elementType, innerIndexOptions.getElementType());
                    elementType = innerIndexOptions.getElementType();
                }

                DenseVectorFieldMapper.DenseVectorIndexOptions denseVectorIndexOptions = innerIndexOptions.getBaseIndexOptions();
                if (denseVectorIndexOptions != null) {
                    int dims = modelSettings.dimensions() != null ? modelSettings.dimensions() : 0;
                    denseVectorIndexOptions.validate(elementType, dims, true);
                }
            }
        }

        @Override
        protected SemanticTextFieldMapper buildMapper(String fullName, ObjectMapper inferenceField, BuilderParams builderParams) {
            return new SemanticTextFieldMapper(
                leafName(),
                new SemanticTextFieldType(
                    fullName,
                    inferenceId.getValue(),
                    searchInferenceId.getValue(),
                    modelSettings.getValue(),
                    chunkingSettings.getValue(),
                    indexOptions.getValue(),
                    inferenceField,
                    useLegacyFormat,
                    meta.getValue()
                ),
                builderParams,
                modelRegistry,
                vectorsFormatProviders
            );
        }

        @Override
        protected Logger logger() {
            return SemanticTextFieldMapper.logger;
        }
    }

    private SemanticTextFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        ModelRegistry modelRegistry,
        List<VectorsFormatProvider> vectorsFormatProviders
    ) {
        super(simpleName, mappedFieldType, builderParams, modelRegistry, vectorsFormatProviders);
    }

    @Override
    public SemanticTextFieldMapper.Builder getMergeBuilder() {
        return new Builder(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final XContentParser parser = context.parser();
        final XContentLocation xContentLocation = parser.getTokenLocation();

        if (fieldType().useLegacyFormat == false) {
            // Detect if field value is an object, which we don't support parsing
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                throw new DocumentParsingException(
                    xContentLocation,
                    "[" + CONTENT_TYPE + "] field [" + fullPath() + "] does not support object values"
                );
            }

            // ignore the rest of the field value
            parser.skipChildren();
            return;
        }

        final SemanticTextField field = parseSemanticTextField(context);
        if (field != null) {
            parseCreateFieldFromContext(context, field, xContentLocation);
        }
    }

    @Override
    protected SemanticTextField.ParserContext getParserContext(DocumentParserContext context) {
        return new SemanticTextField.ParserContext(fieldType().useLegacyFormat, fullPath(), context.parser().contentType());
    }

    @Override
    protected SemanticTextField parseSemanticTextField(DocumentParserContext context) throws IOException {
        SemanticTextField semanticTextField = super.parseSemanticTextField(context);

        IndexVersion indexCreatedVersion = context.indexSettings().getIndexVersionCreated();
        if (semanticTextField != null
            && semanticTextField.inference().modelSettings() != null
            && indexCreatedVersion.before(NEW_SPARSE_VECTOR)) {
            // Explicitly fail to parse semantic text fields that meet the following criteria:
            // - Are in pre 8.11 indices
            // - Have model settings, indicating that they have embeddings to be indexed
            //
            // We can't fail earlier than this because it causes pre 8.11 indices with semantic text fields to either be in red state or
            // cause Elasticsearch to not launch.
            throw new UnsupportedOperationException(UNSUPPORTED_INDEX_MESSAGE);
        }

        return semanticTextField;
    }

    @Override
    protected void parseChunkValueReference(
        DocumentParserContext context,
        SemanticFieldType fieldType,
        String fieldName,
        SemanticTextField.Chunk chunk
    ) throws IOException {
        SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldType) fieldType;
        if (semanticTextFieldType.useLegacyFormat() == false) {
            super.parseChunkValueReference(context, fieldType, fieldName, chunk);
        }
    }

    public static class SemanticTextFieldType extends SemanticFieldType {
        private final boolean useLegacyFormat;

        public SemanticTextFieldType(
            String name,
            String inferenceId,
            String searchInferenceId,
            MinimalServiceSettings modelSettings,
            ChunkingSettings chunkingSettings,
            SemanticTextIndexOptions indexOptions,
            ObjectMapper inferenceField,
            boolean useLegacyFormat,
            Map<String, String> meta
        ) {
            super(name, inferenceId, searchInferenceId, modelSettings, chunkingSettings, indexOptions, inferenceField, meta);
            this.useLegacyFormat = useLegacyFormat;
        }

        public boolean useLegacyFormat() {
            return useLegacyFormat;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String familyTypeName() {
            return TextFieldMapper.CONTENT_TYPE;
        }

        @Override
        public String getDefaultHighlighter() {
            return SemanticTextHighlighter.NAME;
        }

        @Override
        protected ValueFetcher originalValueFetcher(SearchExecutionContext context) {
            return useLegacyFormat
                ? SourceValueFetcher.toString(getOriginalTextFieldName(name()), context, null)
                : super.originalValueFetcher(context);
        }

        @Override
        protected ValueFetcher allValuesFetcher(BlockLoaderContext blContext) {
            if (useLegacyFormat) {
                return SourceValueFetcher.toString(blContext.sourcePaths(getOriginalTextFieldName(name())), blContext.indexSettings());
            }

            return super.allValuesFetcher(blContext);
        }

    }

    private static void configureSparseVectorMapperBuilder(
        IndexVersion indexVersionCreated,
        SparseVectorFieldMapper.Builder sparseVectorMapperBuilder,
        SemanticTextIndexOptions indexOptions
    ) {
        if (indexOptions != null) {
            SparseVectorFieldMapper.SparseVectorIndexOptions sparseVectorIndexOptions =
                (SparseVectorFieldMapper.SparseVectorIndexOptions) indexOptions.indexOptions();

            sparseVectorMapperBuilder.setIndexOptions(sparseVectorIndexOptions);
        } else {
            SparseVectorFieldMapper.SparseVectorIndexOptions defaultIndexOptions = SparseVectorFieldMapper.SparseVectorIndexOptions
                .getDefaultIndexOptions(indexVersionCreated);
            if (defaultIndexOptions != null) {
                sparseVectorMapperBuilder.setIndexOptions(defaultIndexOptions);
            }
        }
    }

    static DenseVectorFieldMapper.DenseVectorIndexOptions defaultDenseVectorIndexOptions(
        IndexVersion indexVersionCreated,
        MinimalServiceSettings modelSettings
    ) {
        if (setExplicitIndexOptionsForSemanticText(indexVersionCreated) == false) {
            return null;
        }
        if (modelSettings.dimensions() == null) {
            return null; // Cannot determine default index options without dimensions
        }

        // As embedding models for text perform better with BBQ, we aggressively default semantic_text fields to use optimized index
        // options
        DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswIndexOptions = defaultBbqHnswDenseVectorIndexOptions();
        return defaultBbqHnswIndexOptions.validate(modelSettings.elementType(), modelSettings.dimensions(), false)
            ? defaultBbqHnswIndexOptions
            : null;
    }

    static boolean setExplicitIndexOptionsForSemanticText(IndexVersion indexVersion) {
        return indexVersion.between(SEMANTIC_TEXT_DEFAULTS_TO_BBQ, SEMANTIC_TEXT_USES_DENSE_VECTOR_DEFAULT_INDEX_OPTIONS)
            || indexVersion.between(SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
    }

    private static boolean defaultElementTypeToBfloat16(
        IndexVersion indexVersionCreated,
        DenseVectorFieldMapper.ElementType modelElementType
    ) {
        return indexVersionCreated.onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16)
            && modelElementType == DenseVectorFieldMapper.ElementType.FLOAT;
    }

    public static DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswDenseVectorIndexOptions() {
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        DenseVectorFieldMapper.RescoreVector rescoreVector = new DenseVectorFieldMapper.RescoreVector(DEFAULT_RESCORE_OVERSAMPLE);
        return new DenseVectorFieldMapper.BBQHnswIndexOptions(m, efConstruction, false, rescoreVector, -1);
    }

    static SemanticTextIndexOptions defaultIndexOptions(IndexVersion indexVersionCreated, MinimalServiceSettings modelSettings) {
        if (modelSettings == null) {
            return null;
        }

        if (modelSettings.taskType() == TEXT_EMBEDDING || modelSettings.taskType() == EMBEDDING) {
            DenseVectorFieldMapper.DenseVectorIndexOptions denseVectorIndexOptions = defaultDenseVectorIndexOptions(
                indexVersionCreated,
                modelSettings
            );
            DenseVectorFieldMapper.ElementType elementType = defaultElementTypeToBfloat16(indexVersionCreated, modelSettings.elementType())
                ? DenseVectorFieldMapper.ElementType.BFLOAT16
                : null;

            return denseVectorIndexOptions == null && elementType == null
                ? null
                : new SemanticTextIndexOptions(
                    SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                    new ExtendedDenseVectorIndexOptions(denseVectorIndexOptions, elementType)
                );
        }

        if (modelSettings.taskType() == SPARSE_EMBEDDING) {
            SparseVectorFieldMapper.SparseVectorIndexOptions sparseVectorIndexOptions = SparseVectorFieldMapper.SparseVectorIndexOptions
                .getDefaultIndexOptions(indexVersionCreated);

            return sparseVectorIndexOptions == null
                ? null
                : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, sparseVectorIndexOptions);
        }

        return null;
    }

    private static boolean useLegacyFormat(IndexSettings indexSettings) {
        return InferenceMetadataFieldsMapper.isEnabled(indexSettings.getSettings()) == false;
    }
}
