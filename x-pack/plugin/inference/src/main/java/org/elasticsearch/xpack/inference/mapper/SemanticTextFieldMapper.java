/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceModelFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.SemanticTextModelSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_RESULTS;

/**
 *  A {@link FieldMapper} for semantic text fields. These fields have a model id reference, that is used for performing inference
 * at ingestion and query time.
 * For now, it is compatible with text expansion models only, but will be extended to support dense vector models as well.
 * This field mapper performs no indexing, as inference results will be included as a different field in the document source, and will
 * be indexed using {@link SemanticTextInferenceResultFieldMapper}.
 */
public class SemanticTextFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "semantic_text";

    private final IndexVersion indexCreatedVersion;

    private static SemanticTextFieldMapper toType(FieldMapper in) {
        return (SemanticTextFieldMapper) in;
    }

    public static final TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated()),
        notInMultiFields(CONTENT_TYPE)
    );

    private SemanticTextFieldMapper(String simpleName, MappedFieldType mappedFieldType, CopyTo copyTo, IndexVersion indexCreatedVersion) {
        super(simpleName, mappedFieldType, MultiFields.empty(), copyTo);
        this.indexCreatedVersion = indexCreatedVersion;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexCreatedVersion).init(this);
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

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<String> modelId = Parameter.stringParam("model_id", false, m -> toType(m).fieldType().modelId, null)
            .addValidator(v -> {
                if (Strings.isEmpty(v)) {
                    throw new IllegalArgumentException("field [model_id] must be specified");
                }
            });

        private final IndexVersion indexCreatedVersion;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name, IndexVersion indexCreatedVersion) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { modelId, meta };
        }

        @Override
        public SemanticTextFieldMapper build(MapperBuilderContext context) {
            return new SemanticTextFieldMapper(
                name(),
                new SemanticTextFieldType(name(), modelId.getValue(), meta.getValue(), indexCreatedVersion),
                copyTo,
                indexCreatedVersion
            );
        }
    }

    public static class SemanticTextFieldType extends SimpleMappedFieldType implements InferenceModelFieldType {

        private final String modelId;

        private final IndexVersion indexVersionCreated;

        public SemanticTextFieldType(String name, String modelId, Map<String, String> meta, IndexVersion indexVersionCreated) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            this.modelId = modelId;
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String getInferenceModel() {
            return modelId;
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

        public Query semanticQuery(
            InferenceResults inferenceResults,
            SemanticTextModelSettings modelSettings,
            SearchExecutionContext context
        ) {
            // Cant use QueryBuilders.boolQuery() because a mapper is not registered for <field>.inference, causing
            // TermQueryBuilder#doToQuery to fail (at TermQueryBuilder:202)
            // TODO: Handle boost and queryName
            String fieldName = name() + "." + INFERENCE_CHUNKS_RESULTS;

            // TODO: Support dense vectors
            if (inferenceResults instanceof TextExpansionResults textExpansionResults) {
                return sparseVectorQuery(fieldName, textExpansionResults, context);
            } else if (inferenceResults instanceof TextEmbeddingResults textEmbeddingResults) {
                return denseVectorQuery(fieldName, textEmbeddingResults, modelSettings, context);
            } else {
                throw new IllegalArgumentException("Unsupported inference results type [" + inferenceResults.getWriteableName() + "]");
            }
        }

        private ESToParentBlockJoinQuery sparseVectorQuery(
            String fieldName,
            TextExpansionResults textExpansionResults,
            SearchExecutionContext context
        ) {
            BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);
            for (TextExpansionResults.WeightedToken weightedToken : textExpansionResults.getWeightedTokens()) {
                queryBuilder.add(
                    new BoostQuery(new TermQuery(new Term(fieldName, weightedToken.token())), weightedToken.weight()),
                    BooleanClause.Occur.SHOULD
                );
            }
            BitSetProducer parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
            return new ESToParentBlockJoinQuery(queryBuilder.build(), parentFilter, ScoreMode.Total, name());
        }

        private Query denseVectorQuery(
            String fieldName,
            TextEmbeddingResults textEmbeddingResults,
            SemanticTextModelSettings modelSettings,
            SearchExecutionContext context
        ) {
            var vectorFieldType = new DenseVectorFieldMapper.DenseVectorFieldType(
                fieldName,
                indexVersionCreated,
                // TODO Add to SemanticTextModelSettings
                DenseVectorFieldMapper.ElementType.FLOAT,
                modelSettings.dimensions(),
                true,
                getSimilarity(modelSettings.similarity()),
                Map.of()
            );

            Query knnQuery = vectorFieldType.createKnnQuery(textEmbeddingResults.getInferenceAsFloat(), 10, null, null, null);
            BitSetProducer parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
            return new ESToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Total, name());
        }

        private static DenseVectorFieldMapper.VectorSimilarity getSimilarity(SimilarityMeasure similarity) {
            return switch (similarity) {
                case COSINE -> DenseVectorFieldMapper.VectorSimilarity.COSINE;
                case DOT_PRODUCT -> DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT;
                default -> throw new IllegalArgumentException("Unsupported similarity measure [" + similarity + "]");
            };
        }
    }
}
