/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMergeContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexVersions.NEW_SPARSE_VECTOR;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_OFFSET_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKING_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOffsetsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOriginalTextFieldName;
import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_ELSER_ID;

/**
 * A {@link FieldMapper} for semantic text fields.
 */
public class SemanticTextFieldMapper extends FieldMapper implements InferenceFieldMapper {
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

    public static final String CONTENT_TYPE = "semantic_text";
    public static final String DEFAULT_FALLBACK_ELSER_INFERENCE_ID = DEFAULT_ELSER_ID;
    public static final String DEFAULT_EIS_ELSER_INFERENCE_ID = DEFAULT_ELSER_ENDPOINT_ID_V2;

    public static final String UNSUPPORTED_INDEX_MESSAGE = "["
        + CONTENT_TYPE
        + "] is available on indices created with 8.11 or higher. Please create a new index to use ["
        + CONTENT_TYPE
        + "]";

    public static final float DEFAULT_RESCORE_OVERSAMPLE = 3.0f;

    /**
     * Determines the preferred ELSER inference ID based on EIS availability.
     * Returns .elser-2-elastic (EIS) when available, otherwise falls back to .elser-2-elasticsearch (ML nodes).
     * This enables automatic selection of EIS for better performance while maintaining compatibility with on-prem deployments.
     */
    private static String getPreferredElserInferenceId(ModelRegistry modelRegistry) {
        if (modelRegistry != null && modelRegistry.containsPreconfiguredInferenceEndpointId(DEFAULT_EIS_ELSER_INFERENCE_ID)) {
            return DEFAULT_EIS_ELSER_INFERENCE_ID;
        }
        return DEFAULT_FALLBACK_ELSER_INFERENCE_ID;
    }

    static final String INDEX_OPTIONS_FIELD = "index_options";

    public static final TypeParser parser(Supplier<ModelRegistry> modelRegistry) {
        return new TypeParser(
            (n, c) -> new Builder(n, c::bitSetProducer, c.getIndexSettings(), modelRegistry.get()),
            List.of(validateParserContext(CONTENT_TYPE))
        );
    }

    public static BiConsumer<String, MappingParserContext> validateParserContext(String type) {
        return (n, c) -> {
            if (InferenceMetadataFieldsMapper.isEnabled(c.getIndexSettings().getSettings()) == false) {
                notInMultiFields(type).accept(n, c);
            }
            notFromDynamicTemplates(type).accept(n, c);
        };
    }

    public static class Builder extends FieldMapper.Builder {
        private final ModelRegistry modelRegistry;
        private final boolean useLegacyFormat;
        private final IndexVersion indexVersionCreated;

        private final Parameter<String> inferenceId;

        private final Parameter<String> searchInferenceId = Parameter.stringParam(
            SEARCH_INFERENCE_ID_FIELD,
            true,
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).searchInferenceId,
            null
        ).acceptsNull().addValidator(v -> {
            if (v != null && Strings.isEmpty(v)) {
                throw new IllegalArgumentException(
                    "[" + SEARCH_INFERENCE_ID_FIELD + "] on mapper [" + leafName() + "] of type [" + CONTENT_TYPE + "] must not be empty"
                );
            }
        });

        private final Parameter<MinimalServiceSettings> modelSettings;

        private final Parameter<SemanticTextIndexOptions> indexOptions;

        @SuppressWarnings("unchecked")
        private final Parameter<ChunkingSettings> chunkingSettings = new Parameter<>(
            CHUNKING_SETTINGS_FIELD,
            true,
            () -> null,
            (n, c, o) -> SemanticTextField.parseChunkingSettingsFromMap(o),
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).chunkingSettings,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull();

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private Function<MapperBuilderContext, ObjectMapper> inferenceFieldBuilder;

        public static Builder from(SemanticTextFieldMapper mapper) {
            Builder builder = new Builder(
                mapper.leafName(),
                mapper.fieldType().getChunksField().bitsetProducer(),
                mapper.fieldType().getChunksField().indexSettings(),
                mapper.modelRegistry
            );
            builder.init(mapper);
            return builder;
        }

        public Builder(
            String name,
            Function<Query, BitSetProducer> bitSetProducer,
            IndexSettings indexSettings,
            ModelRegistry modelRegistry
        ) {
            super(name);
            this.modelRegistry = modelRegistry;
            this.useLegacyFormat = InferenceMetadataFieldsMapper.isEnabled(indexSettings.getSettings()) == false;
            this.indexVersionCreated = indexSettings.getIndexVersionCreated();

            this.inferenceId = Parameter.stringParam(
                INFERENCE_ID_FIELD,
                true,
                mapper -> ((SemanticTextFieldType) mapper.fieldType()).inferenceId,
                getPreferredElserInferenceId(modelRegistry)
            ).addValidator(v -> {
                if (Strings.isEmpty(v)) {
                    throw new IllegalArgumentException(
                        "[" + INFERENCE_ID_FIELD + "] on mapper [" + leafName() + "] of type [" + CONTENT_TYPE + "] must not be empty"
                    );
                }
            }).alwaysSerialize();

            this.modelSettings = new Parameter<>(
                MODEL_SETTINGS_FIELD,
                true,
                () -> null,
                (n, c, o) -> SemanticTextField.parseModelSettingsFromMap(o),
                mapper -> ((SemanticTextFieldType) mapper.fieldType()).modelSettings,
                XContentBuilder::field,
                Objects::toString
            ).acceptsNull().setMergeValidator(SemanticTextFieldMapper::canMergeModelSettings);

            this.indexOptions = new Parameter<>(
                INDEX_OPTIONS_FIELD,
                true,
                () -> null,
                (n, c, o) -> parseIndexOptionsFromMap(n, o, c.indexVersionCreated()),
                mapper -> ((SemanticTextFieldType) mapper.fieldType()).indexOptions,
                (b, n, v) -> {
                    if (v == null) {
                        MinimalServiceSettings resolvedModelSettings = modelSettings.get() != null
                            ? modelSettings.get()
                            : modelRegistry.getMinimalServiceSettings(inferenceId.get());
                        b.field(INDEX_OPTIONS_FIELD, defaultIndexOptions(indexVersionCreated, resolvedModelSettings));
                    } else {
                        b.field(INDEX_OPTIONS_FIELD, v);
                    }
                },
                Objects::toString
            ).acceptsNull();

            this.inferenceFieldBuilder = c -> {
                // Resolve the model setting from the registry if it has not been set yet.
                var resolvedModelSettings = modelSettings.get() != null ? modelSettings.get() : getResolvedModelSettings(c, false);
                return createInferenceField(c, useLegacyFormat, resolvedModelSettings, indexOptions.get(), bitSetProducer, indexSettings);
            };
        }

        public Builder setInferenceId(String id) {
            this.inferenceId.setValue(id);
            return this;
        }

        public Builder setModelSettings(MinimalServiceSettings value) {
            this.modelSettings.setValue(value);
            return this;
        }

        public Builder setChunkingSettings(ChunkingSettings value) {
            this.chunkingSettings.setValue(value);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { inferenceId, searchInferenceId, modelSettings, chunkingSettings, indexOptions, meta };
        }

        @Override
        protected void merge(FieldMapper mergeWith, Conflicts conflicts, MapperMergeContext mapperMergeContext) {
            SemanticTextFieldMapper semanticMergeWith = (SemanticTextFieldMapper) mergeWith;

            final boolean isInferenceIdUpdate = semanticMergeWith.fieldType().inferenceId.equals(inferenceId.get()) == false;
            final boolean hasExplicitModelSettings = modelSettings.get() != null;

            MinimalServiceSettings updatedModelSettings = modelSettings.get();
            if (isInferenceIdUpdate && hasExplicitModelSettings) {
                validateModelsAreCompatibleWhenInferenceIdIsUpdated(semanticMergeWith.fieldType().inferenceId, conflicts);
                // As the mapper previously had explicit model settings, we need to apply to the new merged mapper
                // the resolved model settings if not explicitly set.
                updatedModelSettings = modelRegistry.getMinimalServiceSettings(semanticMergeWith.fieldType().inferenceId);
            }

            semanticMergeWith = copyWithNewModelSettingsIfNotSet(semanticMergeWith, updatedModelSettings, mapperMergeContext);

            // We make sure to merge the inference field first to catch any model conflicts.
            // If inference_id is updated and there are no explicit model settings, we should be
            // able to switch to the new inference field without the need to check for conflicts.
            if (isInferenceIdUpdate == false || hasExplicitModelSettings) {
                mergeInferenceField(mapperMergeContext, semanticMergeWith);
            }

            super.merge(semanticMergeWith, conflicts, mapperMergeContext);
            conflicts.check();
        }

        private void validateModelsAreCompatibleWhenInferenceIdIsUpdated(String newInferenceId, Conflicts conflicts) {
            MinimalServiceSettings currentModelSettings = modelSettings.get();
            MinimalServiceSettings updatedModelSettings = modelRegistry.getMinimalServiceSettings(newInferenceId);
            if (currentModelSettings != null && updatedModelSettings == null) {
                throw new IllegalArgumentException(
                    "Cannot update ["
                        + CONTENT_TYPE
                        + "] field ["
                        + leafName()
                        + "] because inference endpoint ["
                        + newInferenceId
                        + "] does not exist."
                );
            }
            if (canMergeModelSettings(currentModelSettings, updatedModelSettings, conflicts) == false) {
                throw new IllegalArgumentException(
                    "Cannot update ["
                        + CONTENT_TYPE
                        + "] field ["
                        + leafName()
                        + "] because inference endpoint ["
                        + inferenceId.get()
                        + "] with model settings ["
                        + currentModelSettings
                        + "] is not compatible with new inference endpoint ["
                        + newInferenceId
                        + "] with model settings ["
                        + updatedModelSettings
                        + "]."
                );
            }
        }

        private void mergeInferenceField(MapperMergeContext mapperMergeContext, SemanticTextFieldMapper semanticMergeWith) {
            try {
                var context = mapperMergeContext.createChildContext(semanticMergeWith.leafName(), ObjectMapper.Dynamic.FALSE);
                var inferenceField = inferenceFieldBuilder.apply(context.getMapperBuilderContext());
                var mergedInferenceField = inferenceField.merge(semanticMergeWith.fieldType().getInferenceField(), context);
                inferenceFieldBuilder = c -> mergedInferenceField;
            } catch (Exception e) {
                // Wrap errors in nicer messages that hide inference field internals
                String errorMessage = e.getMessage() != null
                    ? e.getMessage().replaceAll(SemanticTextField.getEmbeddingsFieldName(""), "")
                    : "";
                throw new IllegalArgumentException(errorMessage, e);
            }
        }

        /**
         * Returns the {@link MinimalServiceSettings} defined in this builder if set;
         * otherwise, resolves and returns the settings from the registry.
         */
        private MinimalServiceSettings getResolvedModelSettings(MapperBuilderContext context, boolean logWarning) {
            if (context.getMergeReason() == MapperService.MergeReason.MAPPING_RECOVERY) {
                // the model registry is not available yet
                return null;
            }
            try {
                /*
                 * If the model is not already set and we are not in a recovery scenario, resolve it using the registry.
                 * Note: We do not set the model in the mapping at this stage. Instead, the model will be added through
                 * a mapping update during the first ingestion.
                 * This approach allows mappings to reference inference endpoints that may not yet exist.
                 * The only requirement is that the referenced inference endpoint must be available at the time of ingestion.
                 */
                return modelRegistry.getMinimalServiceSettings(inferenceId.get());
            } catch (ResourceNotFoundException exc) {
                if (logWarning) {
                    /* We allow the inference ID to be unregistered at this point.
                     * This will delay the creation of sub-fields, so indexing and querying for this field won't work
                     * until the corresponding inference endpoint is created.
                     */
                    logger.warn(
                        "The field [{}] references an unknown inference ID [{}]. "
                            + "Indexing and querying this field will not work correctly until the corresponding "
                            + "inference endpoint is created.",
                        leafName(),
                        inferenceId.get()
                    );
                }
                return null;
            }
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            if (useLegacyFormat && copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            if (useLegacyFormat && multiFieldsBuilder.hasMultiFields()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support multi-fields");
            }

            var resolvedModelSettings = modelSettings.get();
            if (modelSettings.get() == null) {
                resolvedModelSettings = getResolvedModelSettings(context, true);
            }

            if (modelSettings.get() != null) {
                validateServiceSettings(modelSettings.get(), resolvedModelSettings);
            }

            // If index_options are specified by the user, we will validate them against the model settings to ensure compatibility.
            // We do not serialize or otherwise store model settings at this time, this happens when the underlying vector field is created.
            SemanticTextIndexOptions builderIndexOptions = indexOptions.get();
            if (context.getMergeReason() != MapperService.MergeReason.MAPPING_RECOVERY && builderIndexOptions != null) {
                validateIndexOptions(builderIndexOptions, inferenceId.getValue(), resolvedModelSettings);
            }

            final String fullName = context.buildFullName(leafName());

            if (context.isInNestedContext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + fullName + "] cannot be nested");
            }
            var childContext = context.createChildContext(leafName(), ObjectMapper.Dynamic.FALSE);
            final ObjectMapper inferenceField = inferenceFieldBuilder.apply(childContext);

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
                builderParams(this, context),
                modelRegistry
            );
        }

        private void validateServiceSettings(MinimalServiceSettings settings, MinimalServiceSettings resolved) {
            switch (settings.taskType()) {
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
                        + settings.taskType().name()
                );
            }
            if (resolved != null && settings.canMergeWith(resolved) == false) {
                throw new IllegalArgumentException(
                    "Mismatch between provided and registered inference model settings. "
                        + "Provided: ["
                        + settings
                        + "], Expected: ["
                        + resolved
                        + "]."
                );
            }
        }

        private void validateIndexOptions(SemanticTextIndexOptions indexOptions, String inferenceId, MinimalServiceSettings modelSettings) {
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

                int dims = modelSettings.dimensions() != null ? modelSettings.dimensions() : 0;
                DenseVectorFieldMapper.DenseVectorIndexOptions denseVectorIndexOptions =
                    (DenseVectorFieldMapper.DenseVectorIndexOptions) indexOptions.indexOptions();
                denseVectorIndexOptions.validate(modelSettings.elementType(), dims, true);
            }
        }

        /**
         * Creates a new mapper with the new model settings if model settings are not set on the mapper.
         * If the mapper already has model settings or the new model settings are null, the mapper is
         * returned unchanged.
         *
         * @param mapper        The mapper
         * @param modelSettings the new model settings. If null the mapper will be returned unchanged.
         * @return A mapper with the copied settings applied
         */
        private SemanticTextFieldMapper copyWithNewModelSettingsIfNotSet(
            SemanticTextFieldMapper mapper,
            @Nullable MinimalServiceSettings modelSettings,
            MapperMergeContext mapperMergeContext
        ) {
            SemanticTextFieldMapper returnedMapper = mapper;
            if (mapper.fieldType().getModelSettings() == null) {
                Builder builder = from(mapper);
                builder.setModelSettings(modelSettings);
                returnedMapper = builder.build(mapperMergeContext.getMapperBuilderContext());
            }

            return returnedMapper;
        }
    }

    private final ModelRegistry modelRegistry;

    private SemanticTextFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        ModelRegistry modelRegistry
    ) {
        super(simpleName, mappedFieldType, builderParams);
        ensureMultiFields(builderParams.multiFields().iterator());
        this.modelRegistry = modelRegistry;
    }

    private void ensureMultiFields(Iterator<FieldMapper> mappers) {
        while (mappers.hasNext()) {
            var mapper = mappers.next();
            if (mapper.leafName().equals(INFERENCE_FIELD)) {
                throw new IllegalArgumentException(
                    "Field ["
                        + mapper.fullPath()
                        + "] is already used by another field ["
                        + fullPath()
                        + "] internally. Please choose a different name."
                );
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>();
        Iterator<Mapper> m = super.iterator();
        while (m.hasNext()) {
            mappers.add(m.next());
        }
        mappers.add(fieldType().getInferenceField());
        return mappers.iterator();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return Builder.from(this);
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
        Builder builder = (Builder) getMergeBuilder();
        context.path().remove();
        try {
            builder.setModelSettings(field.inference().modelSettings()).setInferenceId(field.inference().inferenceId());
            if (context.mappingLookup().isMultiField(fullPath())) {
                // The field is part of a multi-field, so the parent field must also be updated accordingly.
                var fieldName = context.path().remove();
                try {
                    var parentMapper = ((FieldMapper) context.mappingLookup().getMapper(context.mappingLookup().parentField(fullPath())))
                        .getMergeBuilder();
                    context.addDynamicMapper(parentMapper.addMultiField(builder).build(context.createDynamicMapperBuilderContext()));
                    return builder.build(context.createDynamicMapperBuilderContext());
                } finally {
                    context.path().add(fieldName);
                }
            } else {
                var mapper = builder.build(context.createDynamicMapperBuilderContext());
                context.addDynamicMapper(mapper);
                return mapper;
            }
        } finally {
            context.path().add(leafName());
        }
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
    public InferenceFieldMetadata getMetadata(Set<String> sourcePaths) {
        String[] copyFields = sourcePaths.toArray(String[]::new);
        // ensure consistent order
        Arrays.sort(copyFields);
        ChunkingSettings fieldTypeChunkingSettings = fieldType().getChunkingSettings();
        Map<String, Object> asMap = fieldTypeChunkingSettings != null ? fieldTypeChunkingSettings.asMap() : null;

        return new InferenceFieldMetadata(fullPath(), fieldType().getInferenceId(), fieldType().getSearchInferenceId(), copyFields, asMap);
    }

    @Override
    protected void doValidate(MappingLookup mappers) {
        String fullPath = mappers.isMultiField(fullPath()) ? mappers.parentField(fullPath()) : fullPath();
        String leafName = mappers.getMapper(fullPath).leafName();
        int parentPathIndex = fullPath.lastIndexOf(leafName);
        if (parentPathIndex > 0) {
            String parentName = fullPath.substring(0, parentPathIndex - 1);
            // Check that the parent object field allows subobjects.
            // Subtract one from the parent path index to omit the trailing dot delimiter.
            ObjectMapper parentMapper = mappers.objectMappers().get(parentName);
            if (parentMapper == null) {
                throw new IllegalStateException(CONTENT_TYPE + " field [" + fullPath() + "] does not have a parent object mapper");
            }

            if (parentMapper.subobjects() == ObjectMapper.Subobjects.DISABLED) {
                throw new IllegalArgumentException(
                    CONTENT_TYPE + " field [" + fullPath() + "] cannot be in an object field with subobjects disabled"
                );
            }
        }
    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType {
        private final String inferenceId;
        private final String searchInferenceId;
        private final MinimalServiceSettings modelSettings;
        private final ChunkingSettings chunkingSettings;
        private final SemanticTextIndexOptions indexOptions;
        private final ObjectMapper inferenceField;
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
            super(name, IndexType.terms(true, false), false, meta);
            this.inferenceId = inferenceId;
            this.searchInferenceId = searchInferenceId;
            this.modelSettings = modelSettings;
            this.chunkingSettings = chunkingSettings;
            this.indexOptions = indexOptions;
            this.inferenceField = inferenceField;
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

        public String getInferenceId() {
            return inferenceId;
        }

        public String getSearchInferenceId() {
            return searchInferenceId == null ? inferenceId : searchInferenceId;
        }

        public MinimalServiceSettings getModelSettings() {
            return modelSettings;
        }

        public ChunkingSettings getChunkingSettings() {
            return chunkingSettings;
        }

        public ObjectMapper getInferenceField() {
            return inferenceField;
        }

        public NestedObjectMapper getChunksField() {
            return (NestedObjectMapper) inferenceField.getMapper(CHUNKS_FIELD);
        }

        public FieldMapper getEmbeddingsField() {
            return (FieldMapper) getChunksField().getMapper(CHUNKED_EMBEDDINGS_FIELD);
        }

        public FieldMapper getOffsetsField() {
            return (FieldMapper) getChunksField().getMapper(CHUNKED_OFFSET_FIELD);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support term query");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            // If this field has never seen inference results (no model settings), there are no values yet
            if (modelSettings == null) {
                return new MatchNoDocsQuery();
            }

            return NestedQueryBuilder.toQuery(
                (c -> getEmbeddingsField().fieldType().existsQuery(c)),
                getChunksFieldName(name()),
                ScoreMode.None,
                false,
                context
            );
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
                return SourceValueFetcher.toString(getOriginalTextFieldName(name()), context, format);
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

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public boolean fieldHasValue(FieldInfos fieldInfos) {
            return fieldInfos.fieldInfo(getEmbeddingsFieldName(name())) != null;
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

                        yield new KnnVectorQueryBuilder(inferenceResultsFieldName, inference, k, null, null, null, null);
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
                baseMessageBuilder.append(
                    " Is the search inference endpoint ["
                        + searchInferenceId
                        + "] compatible with the inference endpoint ["
                        + inferenceId
                        + "]?"
                );
            } else {
                baseMessageBuilder.append(" Has the configuration for inference endpoint [" + inferenceId + "] changed?");
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

    private static ObjectMapper createInferenceField(
        MapperBuilderContext context,
        boolean useLegacyFormat,
        @Nullable MinimalServiceSettings modelSettings,
        @Nullable SemanticTextIndexOptions indexOptions,
        Function<Query, BitSetProducer> bitSetProducer,
        IndexSettings indexSettings
    ) {
        return new ObjectMapper.Builder(INFERENCE_FIELD, Explicit.of(ObjectMapper.Subobjects.ENABLED)).dynamic(ObjectMapper.Dynamic.FALSE)
            .add(createChunksField(useLegacyFormat, modelSettings, indexOptions, bitSetProducer, indexSettings))
            .build(context);
    }

    private static NestedObjectMapper.Builder createChunksField(
        boolean useLegacyFormat,
        @Nullable MinimalServiceSettings modelSettings,
        @Nullable SemanticTextIndexOptions indexOptions,
        Function<Query, BitSetProducer> bitSetProducer,
        IndexSettings indexSettings
    ) {
        NestedObjectMapper.Builder chunksField = new NestedObjectMapper.Builder(
            SemanticTextField.CHUNKS_FIELD,
            indexSettings.getIndexVersionCreated(),
            bitSetProducer,
            indexSettings
        );
        chunksField.dynamic(ObjectMapper.Dynamic.FALSE);
        if (modelSettings != null) {
            chunksField.add(createEmbeddingsField(indexSettings.getIndexVersionCreated(), modelSettings, indexOptions, useLegacyFormat));
        }
        if (useLegacyFormat) {
            var chunkTextField = new KeywordFieldMapper.Builder(TEXT_FIELD, indexSettings).indexed(false).docValues(false);
            chunksField.add(chunkTextField);
        } else {
            chunksField.add(new OffsetSourceFieldMapper.Builder(CHUNKED_OFFSET_FIELD));
        }
        return chunksField;
    }

    private static Mapper.Builder createEmbeddingsField(
        IndexVersion indexVersionCreated,
        MinimalServiceSettings modelSettings,
        SemanticTextIndexOptions indexOptions,
        boolean useLegacyFormat
    ) {
        return switch (modelSettings.taskType()) {
            case SPARSE_EMBEDDING -> {
                SparseVectorFieldMapper.Builder sparseVectorMapperBuilder = new SparseVectorFieldMapper.Builder(
                    CHUNKED_EMBEDDINGS_FIELD,
                    indexVersionCreated,
                    false
                ).setStored(useLegacyFormat == false);

                configureSparseVectorMapperBuilder(indexVersionCreated, sparseVectorMapperBuilder, indexOptions);

                yield sparseVectorMapperBuilder;
            }
            case TEXT_EMBEDDING -> {
                DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                    CHUNKED_EMBEDDINGS_FIELD,
                    indexVersionCreated,
                    false,
                    List.of()
                );

                configureDenseVectorMapperBuilder(indexVersionCreated, denseVectorMapperBuilder, modelSettings, indexOptions);

                yield denseVectorMapperBuilder;
            }
            default -> throw new IllegalArgumentException("Invalid task_type in model_settings [" + modelSettings.taskType().name() + "]");
        };
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

    private static void configureDenseVectorMapperBuilder(
        IndexVersion indexVersionCreated,
        DenseVectorFieldMapper.Builder denseVectorMapperBuilder,
        MinimalServiceSettings modelSettings,
        SemanticTextIndexOptions indexOptions
    ) {
        // Skip setting similarity on pre 8.11 indices. It causes dense vector field creation to fail because similarity can only be set
        // on indexed fields, which is not done by default prior to 8.11. The fact that the dense vector field is partially configured is
        // moot because we will explicitly fail to index docs into this semantic text field anyways.
        if (indexVersionCreated.onOrAfter(NEW_SPARSE_VECTOR)) {
            SimilarityMeasure similarity = modelSettings.similarity();
            if (similarity != null) {
                switch (similarity) {
                    case COSINE -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.COSINE);
                    case DOT_PRODUCT -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT);
                    case L2_NORM -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.L2_NORM);
                    default -> throw new IllegalArgumentException(
                        "Unknown similarity measure in model_settings [" + similarity.name() + "]"
                    );
                }
            }
        }

        denseVectorMapperBuilder.dimensions(modelSettings.dimensions());
        denseVectorMapperBuilder.elementType(modelSettings.elementType());
        // Here is where we persist index_options. If they are specified by the user, we will use those index_options,
        // otherwise we will determine if we can set default index options. If we can't, we won't persist any index_options
        // and the field will use the defaults for the dense_vector field.
        if (indexOptions != null) {
            DenseVectorFieldMapper.DenseVectorIndexOptions denseVectorIndexOptions =
                (DenseVectorFieldMapper.DenseVectorIndexOptions) indexOptions.indexOptions();
            denseVectorMapperBuilder.indexOptions(denseVectorIndexOptions);
            denseVectorIndexOptions.validate(modelSettings.elementType(), modelSettings.dimensions(), true);
        } else {
            DenseVectorFieldMapper.DenseVectorIndexOptions defaultIndexOptions = defaultDenseVectorIndexOptions(
                indexVersionCreated,
                modelSettings
            );
            if (defaultIndexOptions != null) {
                denseVectorMapperBuilder.indexOptions(defaultIndexOptions);
            }
        }
    }

    static DenseVectorFieldMapper.DenseVectorIndexOptions defaultDenseVectorIndexOptions(
        IndexVersion indexVersionCreated,
        MinimalServiceSettings modelSettings
    ) {

        if (modelSettings.dimensions() == null) {
            return null; // Cannot determine default index options without dimensions
        }

        // As embedding models for text perform better with BBQ, we aggressively default semantic_text fields to use optimized index
        // options
        if (indexVersionDefaultsToBbqHnsw(indexVersionCreated)) {
            DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswIndexOptions = defaultBbqHnswDenseVectorIndexOptions();
            return defaultBbqHnswIndexOptions.validate(modelSettings.elementType(), modelSettings.dimensions(), false)
                ? defaultBbqHnswIndexOptions
                : null;
        }

        return null;
    }

    static boolean indexVersionDefaultsToBbqHnsw(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_BBQ)
            || indexVersion.between(SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
    }

    public static DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswDenseVectorIndexOptions() {
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        DenseVectorFieldMapper.RescoreVector rescoreVector = new DenseVectorFieldMapper.RescoreVector(DEFAULT_RESCORE_OVERSAMPLE);
        return new DenseVectorFieldMapper.BBQHnswIndexOptions(m, efConstruction, false, rescoreVector);
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
            return denseVectorIndexOptions == null
                ? null
                : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR, denseVectorIndexOptions);
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

    public static boolean canMergeModelSettings(MinimalServiceSettings previous, MinimalServiceSettings current, Conflicts conflicts) {
        if (previous != null && current != null && previous.canMergeWith(current)) {
            return true;
        }
        if (previous == null || current == null) {
            return true;
        }
        conflicts.addConflict("model_settings", "");
        return false;
    }

    private static SemanticTextIndexOptions parseIndexOptionsFromMap(String fieldName, Object node, IndexVersion indexVersion) {

        if (node == null) {
            return null;
        }

        Map<String, Object> map = XContentMapValues.nodeMapValue(node, INDEX_OPTIONS_FIELD);
        if (map.size() != 1) {
            throw new IllegalArgumentException("Too many index options provided, found [" + map.keySet() + "]");
        }
        Map.Entry<String, Object> entry = map.entrySet().iterator().next();
        SemanticTextIndexOptions.SupportedIndexOptions indexOptions = SemanticTextIndexOptions.SupportedIndexOptions.fromValue(
            entry.getKey()
        );
        @SuppressWarnings("unchecked")
        Map<String, Object> indexOptionsMap = (Map<String, Object>) entry.getValue();
        return new SemanticTextIndexOptions(indexOptions, indexOptions.parseIndexOptions(fieldName, indexOptionsMap, indexVersion));
    }
}
