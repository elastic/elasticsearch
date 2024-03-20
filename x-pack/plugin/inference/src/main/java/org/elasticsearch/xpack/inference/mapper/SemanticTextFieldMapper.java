/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceModelFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper.INFERENCE_CHUNKS_TEXT;
import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper.RESULTS;

/**
 * A {@link FieldMapper} for semantic text fields.
 * These fields have a reference id reference, that is used for performing inference at ingestion and query time.
 * This field mapper performs no indexing, as inference results will be included as a different field in the document source, and will
 * be indexed using {@link InferenceMetadataFieldMapper}.
 */
public class SemanticTextFieldMapper extends FieldMapper {
    private static final Logger logger = LogManager.getLogger(SemanticTextFieldMapper.class);

    public static final String CONTENT_TYPE = "semantic_text";

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()),
        notInMultiFields(CONTENT_TYPE)
    );

    private final IndexVersion indexVersionCreated;
    private final SemanticTextModelSettings modelSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final NestedObjectMapper subMappers;

    private SemanticTextFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        CopyTo copyTo,
        IndexVersion indexVersionCreated,
        IndexAnalyzers indexAnalyzers,
        SemanticTextModelSettings modelSettings,
        NestedObjectMapper subMappers
    ) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
        this.indexVersionCreated = indexVersionCreated;
        this.indexAnalyzers = indexAnalyzers;
        this.modelSettings = modelSettings;
        this.subMappers = subMappers;
    }

    @Override
    public String name() {
        return super.name();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        subIterators.add(subMappers);
        return subIterators.iterator();
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexVersionCreated, indexAnalyzers).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        // Just parses text - no indexing is performed
        context.parser().textOrNull();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SemanticTextFieldType fieldType() {
        return (SemanticTextFieldType) super.fieldType();
    }

    public SemanticTextModelSettings getModelSettings() {
        return modelSettings;
    }

    public NestedObjectMapper getSubMappers() {
        return subMappers;
    }

    public static class Builder extends FieldMapper.Builder {
        private final IndexVersion indexVersionCreated;
        private final IndexAnalyzers indexAnalyzers;

        private final Parameter<String> inferenceId = Parameter.stringParam(
            "inference_id",
            false,
            m -> toType(m).fieldType().inferenceId,
            null
        ).addValidator(v -> {
            if (Strings.isEmpty(v)) {
                throw new IllegalArgumentException("field [inference_id] must be specified");
            }
        });

        @SuppressWarnings("unchecked")
        private final Parameter<SemanticTextModelSettings> modelSettings = new Parameter<>(
            "model_settings",
            true,
            () -> null,
            (n, c, o) -> SemanticTextModelSettings.fromMap(o),
            mapper -> ((SemanticTextFieldMapper) mapper).modelSettings,
            XContentBuilder::field,
            (m) -> m == null ? "null" : Strings.toString(m)
        ).acceptsNull().setMergeValidator(SemanticTextFieldMapper::canMergeModelSettings);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, IndexVersion indexVersionCreated, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexVersionCreated = indexVersionCreated;
            this.indexAnalyzers = indexAnalyzers;
        }

        public Builder setInferenceId(String id) {
            this.inferenceId.setValue(id);
            return this;
        }

        public Builder setModelSettings(SemanticTextModelSettings value) {
            this.modelSettings.setValue(value);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { inferenceId, modelSettings, meta };
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            final String fullName = context.buildFullName(name());
            NestedObjectMapper.Builder nestedBuilder = new NestedObjectMapper.Builder(RESULTS, indexVersionCreated);
            nestedBuilder.dynamic(ObjectMapper.Dynamic.FALSE);
            TextFieldMapper.Builder textMapperBuilder = new TextFieldMapper.Builder(
                INFERENCE_CHUNKS_TEXT,
                indexVersionCreated,
                indexAnalyzers
            ).index(false).store(false);
            if (modelSettings.get() != null) {
                nestedBuilder.add(createInferenceMapperBuilder(INFERENCE_CHUNKS_RESULTS, modelSettings.get(), indexVersionCreated));
            }
            nestedBuilder.add(textMapperBuilder);
            var childContext = context.createChildContext(name(), ObjectMapper.Dynamic.FALSE);
            var subMappers = nestedBuilder.build(childContext);
            return new SemanticTextFieldMapper(
                name(),
                new SemanticTextFieldType(fullName, inferenceId.getValue(), modelSettings.getValue(), subMappers, meta.getValue()),
                copyTo,
                indexVersionCreated,
                indexAnalyzers,
                modelSettings.getValue(),
                subMappers
            );
        }
    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType implements InferenceModelFieldType {
        private final String inferenceId;
        private final SemanticTextModelSettings modelSettings;
        private final NestedObjectMapper subMappers;

        public SemanticTextFieldType(
            String name,
            String modelId,
            SemanticTextModelSettings modelSettings,
            NestedObjectMapper subMappers,
            Map<String, String> meta
        ) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.inferenceId = modelId;
            this.modelSettings = modelSettings;
            this.subMappers = subMappers;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String getInferenceId() {
            return inferenceId;
        }

        public SemanticTextModelSettings getModelSettings() {
            return modelSettings;
        }

        public NestedObjectMapper getSubMappers() {
            return subMappers;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("termQuery not implemented yet");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[semantic_text] fields do not support sorting, scripting or aggregating");
        }
    }

    private static Mapper.Builder createInferenceMapperBuilder(
        String fieldName,
        SemanticTextModelSettings modelSettings,
        IndexVersion indexVersionCreated
    ) {
        return switch (modelSettings.taskType()) {
            case SPARSE_EMBEDDING -> new SparseVectorFieldMapper.Builder(INFERENCE_CHUNKS_RESULTS);
            case TEXT_EMBEDDING -> {
                DenseVectorFieldMapper.Builder denseVectorMapperBuilder = new DenseVectorFieldMapper.Builder(
                    INFERENCE_CHUNKS_RESULTS,
                    indexVersionCreated
                );
                SimilarityMeasure similarity = modelSettings.similarity();
                if (similarity != null) {
                    switch (similarity) {
                        case COSINE -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.COSINE);
                        case DOT_PRODUCT -> denseVectorMapperBuilder.similarity(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT);
                        default -> throw new IllegalArgumentException(
                            "Unknown similarity measure for field [" + fieldName + "] in model settings: " + similarity
                        );
                    }
                }
                Integer dimensions = modelSettings.dimensions();
                denseVectorMapperBuilder.dimensions(dimensions);
                yield denseVectorMapperBuilder;
            }
            default -> throw new IllegalArgumentException(
                "Invalid [task_type] for [" + fieldName + "] in model settings: " + modelSettings.taskType().name()
            );
        };
    }

    @Override
    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (mergeWith instanceof SemanticTextFieldMapper other) {
            if (other.modelSettings != null && other.modelSettings.inferenceId().equals(other.fieldType().getInferenceId()) == false) {
                throw new IllegalArgumentException(
                    "mapper ["
                        + name()
                        + "] refers to different model ids ["
                        + other.modelSettings.inferenceId()
                        + "] and ["
                        + other.fieldType().getInferenceId()
                        + "]"
                );
            }
        }
        super.checkIncomingMergeType(mergeWith);
    }

    static boolean canMergeModelSettings(
        SemanticTextModelSettings previous,
        SemanticTextModelSettings current,
        FieldMapper.Conflicts conflicts
    ) {
        if (Objects.equals(previous, current)) {
            return true;
        }
        if (previous == null) {
            return true;
        }
        if (current == null) {
            conflicts.addConflict("model_settings", "");
            return false;
        }
        conflicts.addConflict("model_settings", "");
        return false;
    }
}
