/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMergeContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
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
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_OFFSET_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKING_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;

public class SemanticFieldMapper extends FieldMapper implements InferenceFieldMapper {
    private static final Logger logger = LogManager.getLogger(SemanticFieldMapper.class);

    public static final String CONTENT_TYPE = "semantic";

    public static final FeatureFlag SEMANTIC_FIELD_FEATURE_FLAG = new FeatureFlag("semantic_field");

    public static final NodeFeature SEMANTIC_FIELD_MAPPER = new NodeFeature("semantic_field.semantic_field_mapper");

    static final String INDEX_OPTIONS_FIELD = "index_options";

    private static final DenseVectorMapperConfigurator DENSE_VECTOR_MAPPER_CONFIGURATOR = new DenseVectorMapperConfigurator(
        (indexVersion, modelElementType) -> defaultElementTypeToBfloat16(modelElementType)
            ? DenseVectorFieldMapper.ElementType.BFLOAT16
            : modelElementType,
        (indexVersion, modelSimilarity) -> modelSimilarity != null ? modelSimilarity.vectorSimilarity() : null,
        (indexVersion, modelSettings) -> null
    );

    public static TypeParser parser(Supplier<ModelRegistry> modelRegistry) {
        return new TypeParser(
            (n, c) -> new Builder(n, c::bitSetProducer, c.getIndexSettings(), modelRegistry.get(), c.getVectorsFormatProviders()),
            List.of(notFromDynamicTemplates(CONTENT_TYPE))
        );
    }

    public static class Builder extends FieldMapper.Builder {
        protected final Function<Query, BitSetProducer> bitSetProducer;
        protected final ModelRegistry modelRegistry;
        protected final IndexSettings indexSettings;
        protected final IndexVersion indexVersionCreated;
        protected final boolean experimentalFeaturesEnabled;
        protected final List<VectorsFormatProvider> vectorsFormatProviders;

        protected final Parameter<String> inferenceId;
        protected final Parameter<String> searchInferenceId;
        protected final Parameter<MinimalServiceSettings> modelSettings;
        protected final Parameter<SemanticTextIndexOptions> indexOptions;
        protected final Parameter<ChunkingSettings> chunkingSettings;
        protected final Parameter<Map<String, String>> meta;

        private ObjectMapper.Builder inferenceFieldBuilder = null;

        public Builder(
            String name,
            Function<Query, BitSetProducer> bitSetProducer,
            IndexSettings indexSettings,
            ModelRegistry modelRegistry,
            List<VectorsFormatProvider> vectorsFormatProviders
        ) {
            super(name);
            this.bitSetProducer = bitSetProducer;
            this.modelRegistry = modelRegistry;
            this.indexSettings = indexSettings;
            this.indexVersionCreated = indexSettings.getIndexVersionCreated();
            this.experimentalFeaturesEnabled = IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.get(indexSettings.getSettings());
            this.vectorsFormatProviders = vectorsFormatProviders;

            this.inferenceId = configureInferenceIdParam();
            this.searchInferenceId = configureSearchInferenceIdParam();
            this.modelSettings = configureModelSettingsParam();
            this.indexOptions = configureIndexOptionsParam();
            this.chunkingSettings = configureChunkingSettingsParam();
            this.meta = configureMetaParam();
        }

        public Builder(SemanticFieldMapper mapper) {
            this(
                mapper.leafName(),
                mapper.fieldType().getChunksField().bitsetProducer(),
                mapper.fieldType().getChunksField().indexSettings(),
                mapper.modelRegistry,
                mapper.vectorsFormatProviders
            );
            init(mapper);
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

        protected Parameter<String> configureInferenceIdParam() {
            return Parameter.stringParam(
                INFERENCE_ID_FIELD,
                true,
                mapper -> ((SemanticFieldType) mapper.fieldType()).inferenceId,
                getDefaultInferenceId()
            ).addValidator(v -> {
                if (Strings.isEmpty(v)) {
                    throw new IllegalArgumentException(
                        "[" + INFERENCE_ID_FIELD + "] on mapper [" + leafName() + "] of type [" + contentType() + "] must not be empty"
                    );
                }
            }).alwaysSerialize();
        }

        protected String getDefaultInferenceId() {
            return null;
        }

        protected Parameter<String> configureSearchInferenceIdParam() {
            return Parameter.stringParam(
                SEARCH_INFERENCE_ID_FIELD,
                true,
                mapper -> ((SemanticFieldType) mapper.fieldType()).searchInferenceId,
                null
            ).acceptsNull().addValidator(v -> {
                if (v != null && Strings.isEmpty(v)) {
                    throw new IllegalArgumentException(
                        "["
                            + SEARCH_INFERENCE_ID_FIELD
                            + "] on mapper ["
                            + leafName()
                            + "] of type ["
                            + contentType()
                            + "] must not be empty"
                    );
                }
            });
        }

        protected Parameter<MinimalServiceSettings> configureModelSettingsParam() {
            return new Parameter<>(
                MODEL_SETTINGS_FIELD,
                true,
                () -> null,
                (n, c, o) -> SemanticTextField.parseModelSettingsFromMap(o),
                mapper -> ((SemanticFieldType) mapper.fieldType()).modelSettings,
                (b, n, v) -> {
                    if (v != null) {
                        b.field(MODEL_SETTINGS_FIELD, v.getFilteredXContentObject());
                    }
                },
                Objects::toString
            ).acceptsNull().setMergeValidator(SemanticFieldMapper::canMergeModelSettings);
        }

        protected Parameter<SemanticTextIndexOptions> configureIndexOptionsParam() {
            return new Parameter<>(
                INDEX_OPTIONS_FIELD,
                true,
                () -> null,
                (n, c, o) -> parseIndexOptionsFromMap(n, o, c.indexVersionCreated(), experimentalFeaturesEnabled),
                mapper -> ((SemanticFieldType) mapper.fieldType()).indexOptions,
                XContentBuilder::field, // TODO: Customize how default index options are serialized
                Objects::toString
            ).acceptsNull();
        }

        protected Parameter<ChunkingSettings> configureChunkingSettingsParam() {
            return new Parameter<>(
                CHUNKING_SETTINGS_FIELD,
                true,
                () -> null,
                (n, c, o) -> SemanticTextField.parseChunkingSettingsFromMap(o),
                mapper -> ((SemanticFieldType) mapper.fieldType()).chunkingSettings,
                XContentBuilder::field,
                Objects::toString
            ).acceptsNull();
        }

        protected Parameter<Map<String, String>> configureMetaParam() {
            return Parameter.metaParam();
        }

        protected ObjectMapper.Builder getInferenceFieldBuilder(MapperBuilderContext context) {
            var resolvedModelSettings = getResolvedModelSettings(context.getMergeReason(), false);
            return new ObjectMapper.Builder(INFERENCE_FIELD, Explicit.of(ObjectMapper.Subobjects.ENABLED)).dynamic(
                ObjectMapper.Dynamic.FALSE
            ).add(createChunksField(resolvedModelSettings));
        }

        /**
         * Returns the {@link MinimalServiceSettings} defined in this builder if set;
         * otherwise, resolves and returns the settings from the registry.
         */
        protected MinimalServiceSettings getResolvedModelSettings(@Nullable MapperService.MergeReason mergeReason, boolean logWarning) {
            if (modelSettings.get() != null) {
                return modelSettings.get();
            }

            if (Objects.equals(mergeReason, MapperService.MergeReason.MAPPING_RECOVERY)) {
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
                    logger().warn(
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

        protected NestedObjectMapper.Builder createChunksField(@Nullable MinimalServiceSettings resolvedModelSettings) {
            NestedObjectMapper.Builder chunksField = new NestedObjectMapper.Builder(
                CHUNKS_FIELD,
                indexVersionCreated,
                bitSetProducer,
                indexSettings
            );
            chunksField.dynamic(ObjectMapper.Dynamic.FALSE);
            if (resolvedModelSettings != null) {
                chunksField.add(createEmbeddingsField(resolvedModelSettings));
            }
            chunksField.add(new OffsetSourceFieldMapper.Builder(CHUNKED_OFFSET_FIELD));
            return chunksField;
        }

        protected Mapper.Builder createEmbeddingsField(MinimalServiceSettings modelSettings) {
            if (modelSettings.taskType() != TaskType.EMBEDDING) {
                throw new IllegalArgumentException("Invalid task_type in model_settings [" + modelSettings.taskType() + "]");
            }

            DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                CHUNKED_EMBEDDINGS_FIELD,
                indexVersionCreated,
                indexSettings.getMode(),
                false,
                experimentalFeaturesEnabled,
                vectorsFormatProviders,
                false
            );
            ExtendedDenseVectorIndexOptions extendedIndexOptions = indexOptions.get() != null
                ? getExtendedDenseVectorIndexOptions(indexOptions.get())
                : null;
            DENSE_VECTOR_MAPPER_CONFIGURATOR.configure(denseVectorMapperBuilder, indexVersionCreated, modelSettings, extendedIndexOptions);

            return denseVectorMapperBuilder;
        }

        protected void setInferenceFieldBuilder(ObjectMapper.Builder inferenceFieldBuilder) {
            this.inferenceFieldBuilder = inferenceFieldBuilder;
        }

        @Override
        protected void mergeFromBuilder(FieldMapper.Builder incoming, Conflicts conflicts, MapperMergeContext mergeContext) {
            Builder semanticIncoming = (Builder) incoming;

            final boolean isInferenceIdUpdate = semanticIncoming.inferenceId.get().equals(inferenceId.get()) == false;
            final boolean hasExplicitModelSettings = modelSettings.get() != null;

            if (isInferenceIdUpdate && hasExplicitModelSettings) {
                validateModelsAreCompatibleWhenInferenceIdIsUpdated(semanticIncoming.inferenceId.get(), conflicts);
                // As the mapper previously had explicit model settings, we need to apply to the new merged mapper
                // the resolved model settings if not explicitly set.
                if (semanticIncoming.modelSettings.get() == null) {
                    semanticIncoming.setModelSettings(modelRegistry.getMinimalServiceSettings(semanticIncoming.inferenceId.get()));
                }
            } else if (semanticIncoming.modelSettings.get() == null && modelSettings.get() != null) {
                semanticIncoming.setModelSettings(modelSettings.get());
            }

            // We make sure to merge the inference field first to catch any model conflicts.
            // If inference_id is updated and there are no explicit model settings, we should be
            // able to switch to the new inference field without the need to check for conflicts.
            if (isInferenceIdUpdate == false || hasExplicitModelSettings) {
                mergeInferenceFieldFromBuilder(mergeContext, semanticIncoming);
            }

            super.mergeFromBuilder(incoming, conflicts, mergeContext);
            conflicts.check();
        }

        private void validateModelsAreCompatibleWhenInferenceIdIsUpdated(String newInferenceId, Conflicts conflicts) {
            MinimalServiceSettings currentModelSettings = modelSettings.get();
            MinimalServiceSettings updatedModelSettings = modelRegistry.getMinimalServiceSettings(newInferenceId);
            if (currentModelSettings != null && updatedModelSettings == null) {
                throw new IllegalArgumentException(
                    "Cannot update ["
                        + contentType()
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
                        + contentType()
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

        private void mergeInferenceFieldFromBuilder(MapperMergeContext mapperMergeContext, Builder semanticIncoming) {
            try {
                var childContext = mapperMergeContext.createChildContext(semanticIncoming.leafName(), ObjectMapper.Dynamic.FALSE);
                var existingObjBuilder = getInferenceFieldBuilder(childContext.getMapperBuilderContext());
                var incomingObjBuilder = semanticIncoming.getInferenceFieldBuilder(childContext.getMapperBuilderContext());
                var mergedBuilder = (ObjectMapper.Builder) existingObjBuilder.mergeWith(incomingObjBuilder, childContext);
                setInferenceFieldBuilder(mergedBuilder);
            } catch (Exception e) {
                String errorMessage = e.getMessage() != null
                    ? e.getMessage().replaceAll(SemanticTextField.getEmbeddingsFieldName(""), "")
                    : "";
                throw new IllegalArgumentException(errorMessage, e);
            }
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { inferenceId, searchInferenceId, modelSettings, chunkingSettings, indexOptions, meta };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public SemanticFieldMapper build(MapperBuilderContext context) {
            var resolvedModelSettings = getResolvedModelSettings(context.getMergeReason(), true);
            if (resolvedModelSettings != null) {
                validateTaskType(resolvedModelSettings);
            }

            // If index_options are specified by the user, we will validate them against the model settings to ensure compatibility.
            // We do not serialize or otherwise store model settings at this time, this happens when the underlying vector field is created.
            if (context.getMergeReason() != MapperService.MergeReason.MAPPING_RECOVERY) {
                validateIndexOptions(resolvedModelSettings);
            }

            final String fullName = context.buildFullName(leafName());
            if (context.isInNestedContext()) {
                throw new IllegalArgumentException(contentType() + " field [" + fullName + "] cannot be nested");
            }
            var childContext = context.createChildContext(leafName(), ObjectMapper.Dynamic.FALSE);
            final ObjectMapper inferenceField = inferenceFieldBuilder != null
                ? inferenceFieldBuilder.build(childContext)
                : getInferenceFieldBuilder(childContext).build(childContext);

            return buildMapper(fullName, inferenceField, builderParams(this, context));
        }

        protected void validateTaskType(MinimalServiceSettings modelSettings) {
            if (modelSettings.taskType() != EMBEDDING) {
                throw new IllegalArgumentException(
                    "Wrong [" + MinimalServiceSettings.TASK_TYPE_FIELD + "], expected " + EMBEDDING + ", got " + modelSettings.taskType()
                );
            }
        }

        protected void validateIndexOptions(MinimalServiceSettings modelSettings) {
            SemanticTextIndexOptions indexOptions = this.indexOptions.get();
            String inferenceId = this.inferenceId.get();

            if (indexOptions == null) {
                return;
            } else if (modelSettings == null) {
                throw new IllegalArgumentException(
                    "Model settings must be set to validate index options for inference ID [" + inferenceId + "]"
                );
            } else if (indexOptions.type() != SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR) {
                throw new IllegalArgumentException(
                    "[" + contentType() + "] field [" + leafName() + "] does not support [" + indexOptions.type() + "] index options"
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
                denseVectorIndexOptions.validate(elementType, modelSettings.dimensions(), true);
            }
        }

        protected SemanticFieldMapper buildMapper(String fullName, ObjectMapper inferenceField, BuilderParams builderParams) {
            return new SemanticFieldMapper(
                leafName(),
                new SemanticFieldType(
                    fullName,
                    inferenceId.getValue(),
                    searchInferenceId.getValue(),
                    modelSettings.getValue(),
                    chunkingSettings.getValue(),
                    indexOptions.getValue(),
                    inferenceField,
                    meta.getValue()
                ),
                builderParams,
                modelRegistry,
                vectorsFormatProviders
            );
        }

        protected Logger logger() {
            return SemanticFieldMapper.logger;
        }

        protected static void validateElementTypeOverride(
            DenseVectorFieldMapper.ElementType modelElementType,
            DenseVectorFieldMapper.ElementType overrideElementType
        ) {
            boolean valid;
            if (modelElementType == DenseVectorFieldMapper.ElementType.FLOAT) {
                valid = overrideElementType == DenseVectorFieldMapper.ElementType.FLOAT
                    || overrideElementType == DenseVectorFieldMapper.ElementType.BFLOAT16;
            } else {
                valid = overrideElementType == modelElementType;
            }

            if (valid == false) {
                throw new IllegalArgumentException(
                    "Model element type [" + modelElementType + "] is incompatible with element type override [" + overrideElementType + "]"
                );
            }
        }
    }

    protected final ModelRegistry modelRegistry;
    protected final List<VectorsFormatProvider> vectorsFormatProviders;

    SemanticFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        ModelRegistry modelRegistry,
        List<VectorsFormatProvider> vectorsFormatProviders
    ) {
        super(simpleName, mappedFieldType, builderParams);
        ensureMultiFields(builderParams.multiFields().iterator());
        this.modelRegistry = modelRegistry;
        this.vectorsFormatProviders = vectorsFormatProviders;
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
    public SemanticFieldMapper.Builder getMergeBuilder() {
        return new Builder(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticFieldType fieldType() {
        return (SemanticFieldType) super.fieldType();
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
                throw new IllegalStateException(contentType() + " field [" + fullPath() + "] does not have a parent object mapper");
            }

            if (parentMapper.subobjects() == ObjectMapper.Subobjects.DISABLED) {
                throw new IllegalArgumentException(
                    contentType() + " field [" + fullPath() + "] cannot be in an object field with subobjects disabled"
                );
            }
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        // Value parsing is handled by parseCreateFieldFromContext
        context.parser().skipChildren();
    }

    protected SemanticTextField.ParserContext getParserContext(DocumentParserContext context) {
        return new SemanticTextField.ParserContext(false, fullPath(), context.parser().contentType());
    }

    protected SemanticTextField parseSemanticTextField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }

        boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            context.path().setWithinLeafObject(true);
            return SemanticTextField.parse(context.parser(), getParserContext(context));
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }
    }

    protected void parseCreateFieldFromContext(DocumentParserContext context, SemanticTextField field, XContentLocation xContentLocation)
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

        final SemanticFieldMapper mapper;
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

                parseChunkValueReference(nestedContext, mapper.fieldType(), entry.getKey(), chunk);
            }
        }
    }

    protected void parseChunkValueReference(
        DocumentParserContext context,
        SemanticFieldType fieldType,
        String fieldName,
        SemanticTextField.Chunk chunk
    ) throws IOException {
        XContentType contentType = context.parser().contentType();
        FieldMapper offsetsField = fieldType.getOffsetsField();
        try (XContentBuilder builder = XContentFactory.contentBuilder(contentType)) {
            builder.startObject();
            builder.field("field", fieldName);
            if (chunk.inputIndex() != null) {
                builder.field("input_index", chunk.inputIndex());
            } else {
                builder.field("start", chunk.startOffset());
                builder.field("end", chunk.endOffset());
            }
            builder.endObject();
            try (
                XContentParser subParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    BytesReference.bytes(builder),
                    contentType
                )
            ) {
                DocumentParserContext subContext = context.switchParser(subParser);
                subParser.nextToken();
                offsetsField.parse(subContext);
            }
        }
    }

    protected SemanticFieldMapper addDynamicUpdate(DocumentParserContext context, SemanticTextField field) {
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
                return (SemanticFieldMapper) context.getDynamicMapper(builder);
            }
        } finally {
            context.path().add(leafName());
        }
    }

    public static class SemanticFieldType extends SimpleMappedFieldType {
        protected final String inferenceId;
        protected final String searchInferenceId;
        protected final MinimalServiceSettings modelSettings;
        protected final ChunkingSettings chunkingSettings;
        protected final SemanticTextIndexOptions indexOptions;
        protected final ObjectMapper inferenceField;

        public SemanticFieldType(
            String name,
            String inferenceId,
            String searchInferenceId,
            MinimalServiceSettings modelSettings,
            ChunkingSettings chunkingSettings,
            SemanticTextIndexOptions indexOptions,
            ObjectMapper inferenceField,
            Map<String, String> meta
        ) {
            super(name, IndexType.terms(true, false), false, meta);
            this.inferenceId = inferenceId;
            this.searchInferenceId = searchInferenceId;
            this.modelSettings = modelSettings;
            this.chunkingSettings = chunkingSettings;
            this.indexOptions = indexOptions;
            this.inferenceField = inferenceField;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
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

        public SemanticTextIndexOptions getIndexOptions() {
            return indexOptions;
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
            throw new IllegalArgumentException(typeName() + " fields do not support term query");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            // If this field has never seen inference results (no model settings), there are no values yet
            if (modelSettings == null) {
                return Queries.NO_DOCS_INSTANCE;
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
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[" + typeName() + "] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public boolean fieldHasValue(FieldInfos fieldInfos) {
            return fieldInfos.fieldInfo(getEmbeddingsFieldName(name())) != null;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null && "chunks".equals(format) == false) {
                throw new IllegalArgumentException(
                    "Unknown format [" + format + "] for field [" + name() + "], only [chunks] is supported."
                );
            }
            if (format != null) {
                return new SemanticFieldValueFetcher(
                    this,
                    getChunksField().bitsetProducer(),
                    context.searcher(),
                    SemanticFieldValueFetcher.Mode.TEXT
                );
            }

            return originalValueFetcher(context);
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new BlockSourceReader.BytesRefsBlockLoader(allValuesFetcher(blContext), BlockSourceReader.lookupMatchingAll());
        }

        /**
         * Get a {@link ValueFetcher} for the original value(s) directly written to this field.
         */
        protected ValueFetcher originalValueFetcher(SearchExecutionContext context) {
            return SourceValueFetcher.toString(name(), context, null);
        }

        /**
         * Get a {@link ValueFetcher} for all values written to this field, both directly and via {@code copy_to}.
         */
        protected ValueFetcher allValuesFetcher(MappedFieldType.BlockLoaderContext blContext) {
            return SourceValueFetcher.toString(blContext.sourcePaths(name()), blContext.indexSettings());
        }

        /**
         * Build a {@link QueryBuilder} that executes a semantic search for the given inference results against this field.
         * The query wraps a task-type-appropriate child query (KNN or sparse vector) inside a {@link NestedQueryBuilder}.
         */
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
                    case TEXT_EMBEDDING, EMBEDDING -> {
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

    protected static SemanticTextIndexOptions parseIndexOptionsFromMap(
        String fieldName,
        Object node,
        IndexVersion indexVersion,
        boolean experimentalFeaturesEnabled
    ) {
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
        return new SemanticTextIndexOptions(
            indexOptions,
            indexOptions.parseIndexOptions(fieldName, indexOptionsMap, indexVersion, experimentalFeaturesEnabled)
        );
    }

    protected static ExtendedDenseVectorIndexOptions getExtendedDenseVectorIndexOptions(SemanticTextIndexOptions indexOptions) {
        IndexOptions innerIndexOptions = indexOptions.indexOptions();
        if (innerIndexOptions instanceof ExtendedDenseVectorIndexOptions edvio) {
            return edvio;
        }

        throw new IllegalStateException("Unexpected inner index options type [" + innerIndexOptions.getClass().getSimpleName() + "]");
    }

    private static boolean defaultElementTypeToBfloat16(DenseVectorFieldMapper.ElementType modelElementType) {
        return modelElementType == DenseVectorFieldMapper.ElementType.FLOAT;
    }
}
