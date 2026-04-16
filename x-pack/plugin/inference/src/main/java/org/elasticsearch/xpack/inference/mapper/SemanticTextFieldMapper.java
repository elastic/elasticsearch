/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
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
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorsFormatProvider;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_OFFSET_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOffsetsFieldName;
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
                case TEXT_EMBEDDING -> {
                    DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                        CHUNKED_EMBEDDINGS_FIELD,
                        indexVersionCreated,
                        false,
                        experimentalFeaturesEnabled,
                        vectorsFormatProviders
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
                case SPARSE_EMBEDDING, TEXT_EMBEDDING -> {
                }
                default -> throw new IllegalArgumentException(
                    "Wrong ["
                        + MinimalServiceSettings.TASK_TYPE_FIELD
                        + "], expected "
                        + TEXT_EMBEDDING
                        + " or "
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
                if (modelSettings.taskType() != TEXT_EMBEDDING) {
                    throw new IllegalArgumentException(
                        "Invalid task type for index options, required [" + TEXT_EMBEDDING + "] but was [" + modelSettings.taskType() + "]"
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

    SemanticTextField parseSemanticTextField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }

        SemanticTextField semanticTextField;
        boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            context.path().setWithinLeafObject(true);
            semanticTextField = SemanticTextField.parse(
                context.parser(),
                new SemanticTextField.ParserContext(fieldType().useLegacyFormat, fullPath(), context.parser().contentType())
            );
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }

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

    void parseCreateFieldFromContext(DocumentParserContext context, SemanticTextField field, XContentLocation xContentLocation)
        throws IOException {
        final String fullFieldName = fieldType().name();
        if (field.inference().inferenceId().equals(fieldType().getInferenceId()) == false) {
            throw new DocumentParsingException(
                xContentLocation,
                Strings.format(
                    "The configured %s [%s] for field [%s] doesn't match the %s [%s] reported in the document.",
                    INFERENCE_ID_FIELD,
                    field.inference().inferenceId(),
                    fullFieldName,
                    INFERENCE_ID_FIELD,
                    fieldType().getInferenceId()
                )
            );
        }

        final SemanticTextFieldMapper mapper;
        if (fieldType().getModelSettings() == null && field.inference().modelSettings() != null) {
            mapper = addDynamicUpdate(context, field);
        } else {
            Conflicts conflicts = new Conflicts(fullFieldName);
            canMergeModelSettings(fieldType().getModelSettings(), field.inference().modelSettings(), conflicts);
            try {
                conflicts.check();
            } catch (Exception exc) {
                throw new DocumentParsingException(
                    xContentLocation,
                    "Incompatible model settings for field ["
                        + fullPath()
                        + "]. Check that the "
                        + INFERENCE_ID_FIELD
                        + " is not using different model settings",
                    exc
                );
            }
            mapper = this;
        }

        if (mapper.fieldType().getModelSettings() == null) {
            for (var chunkList : field.inference().chunks().values()) {
                if (chunkList.isEmpty() == false) {
                    throw new DocumentParsingException(
                        xContentLocation,
                        "[" + MODEL_SETTINGS_FIELD + "] must be set for field [" + fullFieldName + "] when chunks are provided"
                    );
                }
            }
        }

        var chunksField = mapper.fieldType().getChunksField();
        var embeddingsField = mapper.fieldType().getEmbeddingsField();
        var offsetsField = mapper.fieldType().getOffsetsField();
        for (var entry : field.inference().chunks().entrySet()) {
            for (var chunk : entry.getValue()) {
                var nestedContext = context.createNestedContext(chunksField);
                try (
                    XContentParser subParser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY,
                        chunk.rawEmbeddings(),
                        context.parser().contentType()
                    )
                ) {
                    DocumentParserContext subContext = nestedContext.switchParser(subParser);
                    subParser.nextToken();
                    embeddingsField.parse(subContext);
                }

                if (fieldType().useLegacyFormat) {
                    continue;
                }

                try (XContentBuilder builder = XContentFactory.contentBuilder(context.parser().contentType())) {
                    builder.startObject();
                    builder.field("field", entry.getKey());
                    builder.field("start", chunk.startOffset());
                    builder.field("end", chunk.endOffset());
                    builder.endObject();
                    try (
                        XContentParser subParser = XContentHelper.createParserNotCompressed(
                            XContentParserConfiguration.EMPTY,
                            BytesReference.bytes(builder),
                            context.parser().contentType()
                        )
                    ) {
                        DocumentParserContext subContext = nestedContext.switchParser(subParser);
                        subParser.nextToken();
                        offsetsField.parse(subContext);
                    }
                }
            }
        }
    }

    private SemanticTextFieldMapper addDynamicUpdate(DocumentParserContext context, SemanticTextField field) {
        Builder builder = getMergeBuilder();
        context.path().remove();
        try {
            builder.setModelSettings(field.inference().modelSettings()).setInferenceId(field.inference().inferenceId());
            if (context.mappingLookup().isMultiField(fullPath())) {
                // The field is part of a multi-field, so the parent field must also be updated accordingly.
                var fieldName = context.path().remove();
                try {
                    var parentMapper = ((FieldMapper) context.mappingLookup().getMapper(context.mappingLookup().parentField(fullPath())))
                        .getMergeBuilder();
                    String parentFullPath = context.mappingLookup().parentField(fullPath());
                    context.addDynamicMapper(parentMapper.addMultiField(builder), parentFullPath);
                    return builder.build(context.createDynamicMapperBuilderContext());
                } finally {
                    context.path().add(fieldName);
                }
            } else {
                return (SemanticTextFieldMapper) context.getDynamicMapper(builder);
            }
        } finally {
            context.path().add(leafName());
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
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null && "chunks".equals(format) == false) {
                throw new IllegalArgumentException(
                    "Unknown format [" + format + "] for field [" + name() + "], only [chunks] is supported."
                );
            }
            if (format != null) {
                return valueFetcherWithInferenceResults(getChunksField().bitsetProducer(), context.searcher(), true);
            }
            if (useLegacyFormat) {
                // Redirect the fetcher to load the original values of the field
                return SourceValueFetcher.toString(getOriginalTextFieldName(name()), context, null);
            }
            return SourceValueFetcher.toString(name(), context, null);
        }

        ValueFetcher valueFetcherWithInferenceResults(
            Function<Query, BitSetProducer> bitSetCache,
            IndexSearcher searcher,
            boolean onlyTextChunks
        ) {
            var embeddingsField = getEmbeddingsField();
            if (embeddingsField == null) {
                return ValueFetcher.EMPTY;
            }
            try {
                var embeddingsLoader = embeddingsField.syntheticFieldLoader();
                var bitSetFilter = bitSetCache.apply(getChunksField().parentTypeFilter());
                var childWeight = searcher.createWeight(
                    getChunksField().nestedTypeFilter(),
                    org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES,
                    1
                );
                return new SemanticTextFieldValueFetcher(bitSetFilter, childWeight, embeddingsLoader, onlyTextChunks);
            } catch (IOException exc) {
                throw new UncheckedIOException(exc);
            }
        }

        public QueryBuilder semanticQuery(InferenceResults inferenceResults, Integer requestSize, float boost, String queryName) {
            String nestedFieldPath = getChunksFieldName(name());
            String inferenceResultsFieldName = getEmbeddingsFieldName(name());
            QueryBuilder childQueryBuilder;

            if (modelSettings == null) {
                // No inference results have been indexed yet
                childQueryBuilder = new MatchNoneQueryBuilder();
            } else {
                childQueryBuilder = switch (modelSettings.taskType()) {
                    case SPARSE_EMBEDDING -> {
                        if (inferenceResults instanceof TextExpansionResults == false) {
                            throw new IllegalArgumentException(
                                generateQueryInferenceResultsTypeMismatchMessage(inferenceResults, TextExpansionResults.NAME)
                            );
                        }

                        TextExpansionResults textExpansionResults = (TextExpansionResults) inferenceResults;
                        yield new SparseVectorQueryBuilder(
                            inferenceResultsFieldName,
                            textExpansionResults.getWeightedTokens(),
                            null,
                            null,
                            null,
                            null
                        );
                    }
                    case TEXT_EMBEDDING -> {
                        if (inferenceResults instanceof MlDenseEmbeddingResults == false) {
                            throw new IllegalArgumentException(
                                generateQueryInferenceResultsTypeMismatchMessage(inferenceResults, MlDenseEmbeddingResults.NAME)
                            );
                        }

                        MlDenseEmbeddingResults textEmbeddingResults = (MlDenseEmbeddingResults) inferenceResults;
                        float[] inference = textEmbeddingResults.getInferenceAsFloat();
                        int dimensions = modelSettings.elementType() == DenseVectorFieldMapper.ElementType.BIT
                            ? inference.length * Byte.SIZE // Bit vectors encode 8 dimensions into each byte value
                            : inference.length;
                        assert modelSettings.dimensions() != null
                            : "Model settings should have dimensions set by now for text embedding models";
                        if (dimensions != modelSettings.dimensions()) {
                            throw new IllegalArgumentException(
                                generateDimensionCountMismatchMessage(dimensions, modelSettings.dimensions())
                            );
                        }

                        Integer k = requestSize;
                        if (k != null) {
                            // Ensure that k is at least the default size so that aggregations work when size is set to 0 in the request
                            k = Math.max(k, DEFAULT_SIZE);
                        }

                        yield new KnnVectorQueryBuilder(inferenceResultsFieldName, inference, k, null, null, null, null)
                            .setAutoPrefilteringEnabled(true);
                    }
                    default -> throw new IllegalStateException(
                        "Field ["
                            + name()
                            + "] is configured to use an inference endpoint with an unsupported task type ["
                            + modelSettings.taskType()
                            + "]"
                    );
                };
            }

            return new NestedQueryBuilder(nestedFieldPath, childQueryBuilder, ScoreMode.Max).boost(boost).queryName(queryName);
        }

        private String generateQueryInferenceResultsTypeMismatchMessage(InferenceResults inferenceResults, String expectedResultsType) {
            StringBuilder sb = new StringBuilder(
                "Field ["
                    + name()
                    + "] expected query inference results to be of type ["
                    + expectedResultsType
                    + "],"
                    + " got ["
                    + inferenceResults.getWriteableName()
                    + "]."
            );

            return generateInvalidQueryInferenceResultsMessage(sb);
        }

        private String generateDimensionCountMismatchMessage(int inferenceDimCount, int expectedDimCount) {
            StringBuilder sb = new StringBuilder(
                "Field ["
                    + name()
                    + "] expected query inference results with "
                    + expectedDimCount
                    + " dimensions, got "
                    + inferenceDimCount
                    + " dimensions."
            );

            return generateInvalidQueryInferenceResultsMessage(sb);
        }

        private String generateInvalidQueryInferenceResultsMessage(StringBuilder baseMessageBuilder) {
            if (searchInferenceId != null && searchInferenceId.equals(inferenceId) == false) {
                baseMessageBuilder.append(" Is the search inference endpoint [")
                    .append(searchInferenceId)
                    .append("] compatible with the inference endpoint [")
                    .append(inferenceId)
                    .append("]?");
            } else {
                baseMessageBuilder.append(" Has the configuration for inference endpoint [").append(inferenceId).append("] changed?");
            }

            return baseMessageBuilder.toString();
        }

        @Override
        public BlockLoader blockLoader(MappedFieldType.BlockLoaderContext blContext) {
            String name = useLegacyFormat ? name().concat(".text") : name();
            SourceValueFetcher fetcher = SourceValueFetcher.toString(blContext.sourcePaths(name), blContext.indexSettings());
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, BlockSourceReader.lookupMatchingAll());
        }

        private class SemanticTextFieldValueFetcher implements ValueFetcher {
            private final BitSetProducer parentBitSetProducer;
            private final Weight childWeight;
            private final SourceLoader.SyntheticFieldLoader fieldLoader;
            private final boolean onlyTextChunks;

            private BitSet bitSet;
            private Scorer childScorer;
            private SourceLoader.SyntheticFieldLoader.DocValuesLoader dvLoader;
            private OffsetSourceField.OffsetSourceLoader offsetsLoader;

            private SemanticTextFieldValueFetcher(
                BitSetProducer bitSetProducer,
                Weight childWeight,
                SourceLoader.SyntheticFieldLoader fieldLoader,
                boolean onlyTextChunks
            ) {
                this.parentBitSetProducer = bitSetProducer;
                this.childWeight = childWeight;
                this.fieldLoader = fieldLoader;
                this.onlyTextChunks = onlyTextChunks;
            }

            @Override
            public void setNextReader(LeafReaderContext context) {
                try {
                    bitSet = parentBitSetProducer.getBitSet(context);
                    childScorer = childWeight.scorer(context);
                    if (childScorer != null) {
                        childScorer.iterator().nextDoc();
                    }
                    if (onlyTextChunks == false) {
                        dvLoader = fieldLoader.docValuesLoader(context.reader(), null);
                    }
                    var terms = context.reader().terms(getOffsetsFieldName(name()));
                    offsetsLoader = terms != null ? OffsetSourceField.loader(terms) : null;
                } catch (IOException exc) {
                    throw new UncheckedIOException(exc);
                }
            }

            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                if (childScorer == null || offsetsLoader == null || doc == 0) {
                    return List.of();
                }
                int previousParent = bitSet.prevSetBit(doc - 1);
                var it = childScorer.iterator();
                if (it.docID() < previousParent) {
                    it.advance(previousParent);
                }

                return onlyTextChunks ? fetchTextChunks(source, doc, it) : fetchFullField(source, doc, it);
            }

            private List<Object> fetchTextChunks(Source source, int doc, DocIdSetIterator it) throws IOException {
                Map<String, String> originalValueMap = new HashMap<>();
                List<Object> chunks = new ArrayList<>();

                iterateChildDocs(doc, it, offset -> {
                    var rawValue = originalValueMap.computeIfAbsent(offset.field(), k -> {
                        var valueObj = XContentMapValues.extractValue(offset.field(), source.source(), null);
                        var values = SemanticTextUtils.nodeStringValues(offset.field(), valueObj).stream().toList();
                        return Strings.collectionToDelimitedString(values, String.valueOf(MULTIVAL_SEP_CHAR));
                    });

                    chunks.add(rawValue.substring(offset.start(), offset.end()));
                });

                return chunks;
            }

            private List<Object> fetchFullField(Source source, int doc, DocIdSetIterator it) throws IOException {
                Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();

                iterateChildDocs(doc, it, offset -> {
                    var fullChunks = chunkMap.computeIfAbsent(offset.field(), k -> new ArrayList<>());
                    fullChunks.add(
                        new SemanticTextField.Chunk(
                            null,
                            offset.start(),
                            offset.end(),
                            rawEmbeddings(fieldLoader::write, source.sourceContentType())
                        )
                    );
                });

                if (chunkMap.isEmpty()) {
                    return List.of();
                }

                return List.of(
                    new SemanticTextField(
                        useLegacyFormat,
                        name(),
                        null,
                        new SemanticTextField.InferenceResult(inferenceId, modelSettings, chunkingSettings, chunkMap),
                        source.sourceContentType()
                    )
                );
            }

            /**
             * Iterates over all child documents for the given doc and applies the provided action for each valid offset.
             */
            private void iterateChildDocs(
                int doc,
                DocIdSetIterator it,
                CheckedConsumer<OffsetSourceFieldMapper.OffsetSource, IOException> action
            ) throws IOException {
                while (it.docID() < doc) {
                    if (onlyTextChunks == false) {
                        if (dvLoader == null || dvLoader.advanceToDoc(it.docID()) == false) {
                            throw new IllegalStateException(
                                "Cannot fetch values for field [" + name() + "], missing embeddings for doc [" + doc + "]"
                            );
                        }
                    }

                    var offset = offsetsLoader.advanceTo(it.docID());
                    if (offset == null) {
                        throw new IllegalStateException(
                            "Cannot fetch values for field [" + name() + "], missing offsets for doc [" + doc + "]"
                        );
                    }

                    action.accept(offset);

                    if (it.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                        break;
                    }
                }
            }

            private BytesReference rawEmbeddings(CheckedConsumer<XContentBuilder, IOException> writer, XContentType xContentType)
                throws IOException {
                try (var result = XContentFactory.contentBuilder(xContentType)) {
                    try (var builder = XContentFactory.contentBuilder(xContentType)) {
                        builder.startObject();
                        writer.accept(builder);
                        builder.endObject();
                        try (
                            XContentParser parser = XContentHelper.createParserNotCompressed(
                                XContentParserConfiguration.EMPTY,
                                BytesReference.bytes(builder),
                                xContentType
                            )
                        ) {
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                            parser.nextToken();
                            result.copyCurrentStructure(parser);
                        }
                        return BytesReference.bytes(result);
                    }
                }
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
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

        if (modelSettings.taskType() == TaskType.TEXT_EMBEDDING) {
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
