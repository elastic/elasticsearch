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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
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
import org.elasticsearch.index.mapper.TextSearchInfo;
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
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
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

    public static final String CONTENT_TYPE = "semantic_text";
    public static final String DEFAULT_ELSER_2_INFERENCE_ID = DEFAULT_ELSER_ID;

    public static final float DEFAULT_RESCORE_OVERSAMPLE = 3.0f;

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

        private final Parameter<String> inferenceId = Parameter.stringParam(
            INFERENCE_ID_FIELD,
            false,
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).inferenceId,
            DEFAULT_ELSER_2_INFERENCE_ID
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException(
                    "[" + INFERENCE_ID_FIELD + "] on mapper [" + leafName() + "] of type [" + CONTENT_TYPE + "] must not be empty"
                );
            }
        }).alwaysSerialize();

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

        private final Parameter<MinimalServiceSettings> modelSettings = new Parameter<>(
            MODEL_SETTINGS_FIELD,
            true,
            () -> null,
            (n, c, o) -> SemanticTextField.parseModelSettingsFromMap(o),
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).modelSettings,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull().setMergeValidator(SemanticTextFieldMapper::canMergeModelSettings);

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

        private MinimalServiceSettings resolvedModelSettings;
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
            this.inferenceFieldBuilder = c -> createInferenceField(
                c,
                indexSettings.getIndexVersionCreated(),
                useLegacyFormat,
                resolvedModelSettings,
                bitSetProducer,
                indexSettings
            );
        }

        public Builder setInferenceId(String id) {
            this.inferenceId.setValue(id);
            return this;
        }

        public Builder setSearchInferenceId(String id) {
            this.searchInferenceId.setValue(id);
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
            return new Parameter<?>[] { inferenceId, searchInferenceId, modelSettings, chunkingSettings, meta };
        }

        @Override
        protected void merge(FieldMapper mergeWith, Conflicts conflicts, MapperMergeContext mapperMergeContext) {
            SemanticTextFieldMapper semanticMergeWith = (SemanticTextFieldMapper) mergeWith;
            semanticMergeWith = copySettings(semanticMergeWith, mapperMergeContext);

            super.merge(semanticMergeWith, conflicts, mapperMergeContext);
            conflicts.check();
            var context = mapperMergeContext.createChildContext(semanticMergeWith.leafName(), ObjectMapper.Dynamic.FALSE);
            var inferenceField = inferenceFieldBuilder.apply(context.getMapperBuilderContext());
            var mergedInferenceField = inferenceField.merge(semanticMergeWith.fieldType().getInferenceField(), context);
            inferenceFieldBuilder = c -> mergedInferenceField;
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            if (useLegacyFormat && copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            if (useLegacyFormat && multiFieldsBuilder.hasMultiFields()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support multi-fields");
            }

            if (context.getMergeReason() != MapperService.MergeReason.MAPPING_RECOVERY && modelSettings.get() == null) {
                try {
                    /*
                     * If the model is not already set and we are not in a recovery scenario, resolve it using the registry.
                     * Note: We do not set the model in the mapping at this stage. Instead, the model will be added through
                     * a mapping update during the first ingestion.
                     * This approach allows mappings to reference inference endpoints that may not yet exist.
                     * The only requirement is that the referenced inference endpoint must be available at the time of ingestion.
                     */
                    resolvedModelSettings = modelRegistry.getMinimalServiceSettings(inferenceId.get());
                } catch (ResourceNotFoundException exc) {
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
            } else {
                resolvedModelSettings = modelSettings.get();
            }

            if (modelSettings.get() != null) {
                validateServiceSettings(modelSettings.get(), resolvedModelSettings);
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

        /**
         * As necessary, copy settings from this builder to the passed-in mapper.
         * Used to preserve {@link MinimalServiceSettings} when updating a semantic text mapping to one where the model settings
         * are not specified.
         *
         * @param mapper The mapper
         * @return A mapper with the copied settings applied
         */
        private SemanticTextFieldMapper copySettings(SemanticTextFieldMapper mapper, MapperMergeContext mapperMergeContext) {
            SemanticTextFieldMapper returnedMapper = mapper;
            if (mapper.fieldType().getModelSettings() == null) {
                Builder builder = from(mapper);
                builder.setModelSettings(modelSettings.getValue());
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
        boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            context.path().setWithinLeafObject(true);
            return SemanticTextField.parse(
                context.parser(),
                new SemanticTextField.ParserContext(fieldType().useLegacyFormat, fullPath(), context.parser().contentType())
            );
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }
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
        private final ObjectMapper inferenceField;
        private final boolean useLegacyFormat;

        public SemanticTextFieldType(
            String name,
            String inferenceId,
            String searchInferenceId,
            MinimalServiceSettings modelSettings,
            ChunkingSettings chunkingSettings,
            ObjectMapper inferenceField,
            boolean useLegacyFormat,
            Map<String, String> meta
        ) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.inferenceId = inferenceId;
            this.searchInferenceId = searchInferenceId;
            this.modelSettings = modelSettings;
            this.chunkingSettings = chunkingSettings;
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
            if (getEmbeddingsField() == null) {
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
            if (useLegacyFormat) {
                // Redirect the fetcher to load the original values of the field
                return SourceValueFetcher.toString(getOriginalTextFieldName(name()), context, format);
            }
            return SourceValueFetcher.toString(name(), context, null);
        }

        ValueFetcher valueFetcherWithInferenceResults(Function<Query, BitSetProducer> bitSetCache, IndexSearcher searcher) {
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
                return new SemanticTextFieldValueFetcher(bitSetFilter, childWeight, embeddingsLoader);
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
                        if (inferenceResults instanceof MlTextEmbeddingResults == false) {
                            throw new IllegalArgumentException(
                                generateQueryInferenceResultsTypeMismatchMessage(inferenceResults, MlTextEmbeddingResults.NAME)
                            );
                        }

                        MlTextEmbeddingResults textEmbeddingResults = (MlTextEmbeddingResults) inferenceResults;
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

                        yield new KnnVectorQueryBuilder(inferenceResultsFieldName, inference, k, null, null, null);
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
            SourceValueFetcher fetcher = SourceValueFetcher.toString(blContext.sourcePaths(name));
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, BlockSourceReader.lookupMatchingAll());
        }

        private class SemanticTextFieldValueFetcher implements ValueFetcher {
            private final BitSetProducer parentBitSetProducer;
            private final Weight childWeight;
            private final SourceLoader.SyntheticFieldLoader fieldLoader;

            private BitSet bitSet;
            private Scorer childScorer;
            private SourceLoader.SyntheticFieldLoader.DocValuesLoader dvLoader;
            private OffsetSourceField.OffsetSourceLoader offsetsLoader;

            private SemanticTextFieldValueFetcher(
                BitSetProducer bitSetProducer,
                Weight childWeight,
                SourceLoader.SyntheticFieldLoader fieldLoader
            ) {
                this.parentBitSetProducer = bitSetProducer;
                this.childWeight = childWeight;
                this.fieldLoader = fieldLoader;
            }

            @Override
            public void setNextReader(LeafReaderContext context) {
                try {
                    bitSet = parentBitSetProducer.getBitSet(context);
                    childScorer = childWeight.scorer(context);
                    if (childScorer != null) {
                        childScorer.iterator().nextDoc();
                    }
                    dvLoader = fieldLoader.docValuesLoader(context.reader(), null);
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
                Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();
                while (it.docID() < doc) {
                    if (dvLoader == null || dvLoader.advanceToDoc(it.docID()) == false) {
                        throw new IllegalStateException(
                            "Cannot fetch values for field [" + name() + "], missing embeddings for doc [" + doc + "]"
                        );
                    }
                    var offset = offsetsLoader.advanceTo(it.docID());
                    if (offset == null) {
                        throw new IllegalStateException(
                            "Cannot fetch values for field [" + name() + "], missing offsets for doc [" + doc + "]"
                        );
                    }
                    var chunks = chunkMap.computeIfAbsent(offset.field(), k -> new ArrayList<>());
                    chunks.add(
                        new SemanticTextField.Chunk(
                            null,
                            offset.start(),
                            offset.end(),
                            rawEmbeddings(fieldLoader::write, source.sourceContentType())
                        )
                    );
                    if (it.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                        break;
                    }
                }
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
        IndexVersion indexVersionCreated,
        boolean useLegacyFormat,
        @Nullable MinimalServiceSettings modelSettings,
        Function<Query, BitSetProducer> bitSetProducer,
        IndexSettings indexSettings
    ) {
        return new ObjectMapper.Builder(INFERENCE_FIELD, Optional.of(ObjectMapper.Subobjects.ENABLED)).dynamic(ObjectMapper.Dynamic.FALSE)
            .add(createChunksField(indexVersionCreated, useLegacyFormat, modelSettings, bitSetProducer, indexSettings))
            .build(context);
    }

    private static NestedObjectMapper.Builder createChunksField(
        IndexVersion indexVersionCreated,
        boolean useLegacyFormat,
        @Nullable MinimalServiceSettings modelSettings,
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
            chunksField.add(createEmbeddingsField(indexSettings.getIndexVersionCreated(), modelSettings, useLegacyFormat));
        }
        if (useLegacyFormat) {
            var chunkTextField = new KeywordFieldMapper.Builder(TEXT_FIELD, indexVersionCreated).indexed(false).docValues(false);
            chunksField.add(chunkTextField);
        } else {
            chunksField.add(new OffsetSourceFieldMapper.Builder(CHUNKED_OFFSET_FIELD));
        }
        return chunksField;
    }

    private static Mapper.Builder createEmbeddingsField(
        IndexVersion indexVersionCreated,
        MinimalServiceSettings modelSettings,
        boolean useLegacyFormat
    ) {
        return switch (modelSettings.taskType()) {
            case SPARSE_EMBEDDING -> new SparseVectorFieldMapper.Builder(CHUNKED_EMBEDDINGS_FIELD).setStored(useLegacyFormat == false);
            case TEXT_EMBEDDING -> {
                DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                    CHUNKED_EMBEDDINGS_FIELD,
                    indexVersionCreated
                );

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
                denseVectorMapperBuilder.dimensions(modelSettings.dimensions());
                denseVectorMapperBuilder.elementType(modelSettings.elementType());

                DenseVectorFieldMapper.IndexOptions defaultIndexOptions = null;
                if (indexVersionCreated.onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_BBQ)
                    || indexVersionCreated.between(SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X, IndexVersions.UPGRADE_TO_LUCENE_10_0_0)) {
                    defaultIndexOptions = defaultSemanticDenseIndexOptions();
                }
                if (defaultIndexOptions != null
                    && defaultIndexOptions.validate(modelSettings.elementType(), modelSettings.dimensions(), false)) {
                    denseVectorMapperBuilder.indexOptions(defaultIndexOptions);
                }

                yield denseVectorMapperBuilder;
            }
            default -> throw new IllegalArgumentException("Invalid task_type in model_settings [" + modelSettings.taskType().name() + "]");
        };
    }

    static DenseVectorFieldMapper.IndexOptions defaultSemanticDenseIndexOptions() {
        // As embedding models for text perform better with BBQ, we aggressively default semantic_text fields to use optimized index
        // options outside of dense_vector defaults
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        DenseVectorFieldMapper.RescoreVector rescoreVector = new DenseVectorFieldMapper.RescoreVector(DEFAULT_RESCORE_OVERSAMPLE);
        return new DenseVectorFieldMapper.BBQHnswIndexOptions(m, efConstruction, rescoreVector);
    }

    private static boolean canMergeModelSettings(MinimalServiceSettings previous, MinimalServiceSettings current, Conflicts conflicts) {
        if (previous != null && current != null && previous.canMergeWith(current)) {
            return true;
        }
        if (previous == null || current == null) {
            return true;
        }
        conflicts.addConflict("model_settings", "");
        return false;
    }
}
