/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMergeContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOriginalTextFieldName;

/**
 * A {@link FieldMapper} for semantic text fields.
 */
public class SemanticTextFieldMapper extends FieldMapper implements InferenceFieldMapper {
    public static final String CONTENT_TYPE = "semantic_text";

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    public static class Builder extends FieldMapper.Builder {
        private final IndexVersion indexVersionCreated;

        private final Parameter<String> inferenceId = Parameter.stringParam(
            "inference_id",
            false,
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).inferenceId,
            null
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException("field [inference_id] must be specified");
            }
        });

        private final Parameter<SemanticTextField.ModelSettings> modelSettings = new Parameter<>(
            "model_settings",
            true,
            () -> null,
            (n, c, o) -> SemanticTextField.parseModelSettingsFromMap(o),
            mapper -> ((SemanticTextFieldType) mapper.fieldType()).modelSettings,
            XContentBuilder::field,
            Objects::toString
        ).acceptsNull().setMergeValidator(SemanticTextFieldMapper::canMergeModelSettings);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private Function<MapperBuilderContext, ObjectMapper> inferenceFieldBuilder;

        public Builder(String name, IndexVersion indexVersionCreated) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
            this.inferenceFieldBuilder = c -> createInferenceField(c, indexVersionCreated, modelSettings.get());
        }

        public Builder setInferenceId(String id) {
            this.inferenceId.setValue(id);
            return this;
        }

        public Builder setModelSettings(SemanticTextField.ModelSettings value) {
            this.modelSettings.setValue(value);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { inferenceId, modelSettings, meta };
        }

        @Override
        protected void merge(FieldMapper mergeWith, Conflicts conflicts, MapperMergeContext mapperMergeContext) {
            super.merge(mergeWith, conflicts, mapperMergeContext);
            conflicts.check();
            var semanticMergeWith = (SemanticTextFieldMapper) mergeWith;
            var context = mapperMergeContext.createChildContext(mergeWith.simpleName(), ObjectMapper.Dynamic.FALSE);
            var inferenceField = inferenceFieldBuilder.apply(context.getMapperBuilderContext());
            var mergedInferenceField = inferenceField.merge(semanticMergeWith.fieldType().getInferenceField(), context);
            inferenceFieldBuilder = c -> mergedInferenceField;
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name() + "] does not support [copy_to]");
            }
            if (multiFieldsBuilder.hasMultiFields()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name() + "] does not support multi-fields");
            }
            final String fullName = context.buildFullName(name());
            var childContext = context.createChildContext(name(), ObjectMapper.Dynamic.FALSE);
            final ObjectMapper inferenceField = inferenceFieldBuilder.apply(childContext);
            return new SemanticTextFieldMapper(
                name(),
                new SemanticTextFieldType(
                    fullName,
                    inferenceId.getValue(),
                    modelSettings.getValue(),
                    inferenceField,
                    indexVersionCreated,
                    meta.getValue()
                ),
                copyTo
            );
        }
    }

    private SemanticTextFieldMapper(String simpleName, MappedFieldType mappedFieldType, CopyTo copyTo) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(fieldType().getInferenceField());
        return subIterators.iterator();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), fieldType().indexVersionCreated).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        XContentLocation xContentLocation = parser.getTokenLocation();
        final SemanticTextField field;
        boolean isWithinLeaf = context.path().isWithinLeafObject();
        try {
            context.path().setWithinLeafObject(true);
            field = SemanticTextField.parse(parser, new Tuple<>(name(), context.parser().contentType()));
        } finally {
            context.path().setWithinLeafObject(isWithinLeaf);
        }
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
        if (fieldType().getModelSettings() == null) {
            context.path().remove();
            Builder builder = (Builder) new Builder(simpleName(), fieldType().indexVersionCreated).init(this);
            try {
                mapper = builder.setModelSettings(field.inference().modelSettings())
                    .setInferenceId(field.inference().inferenceId())
                    .build(context.createDynamicMapperBuilderContext());
                context.addDynamicMapper(mapper);
            } finally {
                context.path().add(simpleName());
            }
        } else {
            Conflicts conflicts = new Conflicts(fullFieldName);
            canMergeModelSettings(field.inference().modelSettings(), fieldType().getModelSettings(), conflicts);
            try {
                conflicts.check();
            } catch (Exception exc) {
                throw new DocumentParsingException(
                    xContentLocation,
                    "Incompatible model settings for field ["
                        + name()
                        + "]. Check that the "
                        + INFERENCE_ID_FIELD
                        + " is not using different model settings",
                    exc
                );
            }
            mapper = this;
        }
        var chunksField = mapper.fieldType().getChunksField();
        var embeddingsField = mapper.fieldType().getEmbeddingsField();
        for (var chunk : field.inference().chunks()) {
            try (
                XContentParser subParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    chunk.rawEmbeddings(),
                    context.parser().contentType()
                )
            ) {
                DocumentParserContext subContext = context.createNestedContext(chunksField).switchParser(subParser);
                subParser.nextToken();
                embeddingsField.parse(subContext);
            }
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
        return new InferenceFieldMetadata(name(), fieldType().inferenceId, copyFields);
    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType {
        private final String inferenceId;
        private final SemanticTextField.ModelSettings modelSettings;
        private final ObjectMapper inferenceField;
        private final IndexVersion indexVersionCreated;

        public SemanticTextFieldType(
            String name,
            String modelId,
            SemanticTextField.ModelSettings modelSettings,
            ObjectMapper inferenceField,
            IndexVersion indexVersionCreated,
            Map<String, String> meta
        ) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.inferenceId = modelId;
            this.modelSettings = modelSettings;
            this.inferenceField = inferenceField;
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String getInferenceId() {
            return inferenceId;
        }

        public SemanticTextField.ModelSettings getModelSettings() {
            return modelSettings;
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

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException(CONTENT_TYPE + " fields do not support term query");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Redirect the fetcher to load the original values of the field
            return SourceValueFetcher.toString(getOriginalTextFieldName(name()), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }
    }

    private static ObjectMapper createInferenceField(
        MapperBuilderContext context,
        IndexVersion indexVersionCreated,
        @Nullable SemanticTextField.ModelSettings modelSettings
    ) {
        return new ObjectMapper.Builder(INFERENCE_FIELD, Explicit.EXPLICIT_TRUE).dynamic(ObjectMapper.Dynamic.FALSE)
            .add(createChunksField(indexVersionCreated, modelSettings))
            .build(context);
    }

    private static NestedObjectMapper.Builder createChunksField(
        IndexVersion indexVersionCreated,
        SemanticTextField.ModelSettings modelSettings
    ) {
        NestedObjectMapper.Builder chunksField = new NestedObjectMapper.Builder(CHUNKS_FIELD, indexVersionCreated);
        chunksField.dynamic(ObjectMapper.Dynamic.FALSE);
        KeywordFieldMapper.Builder chunkTextField = new KeywordFieldMapper.Builder(CHUNKED_TEXT_FIELD, indexVersionCreated).indexed(false)
            .docValues(false);
        if (modelSettings != null) {
            chunksField.add(createEmbeddingsField(indexVersionCreated, modelSettings));
        }
        chunksField.add(chunkTextField);
        return chunksField;
    }

    private static Mapper.Builder createEmbeddingsField(IndexVersion indexVersionCreated, SemanticTextField.ModelSettings modelSettings) {
        return switch (modelSettings.taskType()) {
            case SPARSE_EMBEDDING -> new SparseVectorFieldMapper.Builder(CHUNKED_EMBEDDINGS_FIELD);
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
                yield denseVectorMapperBuilder;
            }
            default -> throw new IllegalArgumentException("Invalid task_type in model_settings [" + modelSettings.taskType().name() + "]");
        };
    }

    private static boolean canMergeModelSettings(
        SemanticTextField.ModelSettings previous,
        SemanticTextField.ModelSettings current,
        Conflicts conflicts
    ) {
        if (Objects.equals(previous, current)) {
            return true;
        }
        if (previous == null) {
            return true;
        }
        conflicts.addConflict("model_settings", "");
        return false;
    }
}
