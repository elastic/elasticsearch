/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic_query";

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private final String fieldName;
    private final String query;

    private SetOnce<InferenceServiceResults> inferenceResultsSupplier;
    private InferenceServiceResults inferenceResults;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
        if (in.readBoolean()) {
            inferenceResults = in.readNamedWriteable(InferenceServiceResults.class);
            // The supplier is generally not used after the results are set, but set the supplier to maintain equality when serializing
            // & deserializing
            inferenceResultsSupplier = new SetOnce<>(inferenceResults);
        }
    }

    private SemanticQueryBuilder(SemanticQueryBuilder other, SetOnce<InferenceServiceResults> inferenceResultsSupplier) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SEMANTIC_TEXT_FIELD_ADDED;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceResultsSupplier != null && inferenceResults == null) {
            throw new IllegalStateException("Inference results supplier is set, but inference results is null. Missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(query);
        if (inferenceResults != null) {
            out.writeBoolean(true);
            out.writeNamedWriteable(inferenceResults);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // We cannot fully rewrite the query to a NestedQueryBuilder here because that query builder validates that the path is
        // registered as a nested object. This is not the case for semantic_text fields; the SemanticTextInferenceResultFieldMapper
        // metafield mapper indexes inference results for the field using a "shadow" nested field mapping that is coordinated between
        // this query and the mapper. This "shadow" mapping is not registered with the ES index mappings.
        //
        // Instead, we extract the inference results from the supplier in a serializable format and handle creation of the Lucene query
        // in this class in doToQuery.
        if (inferenceResults != null) {
            return this;
        }

        if (inferenceResultsSupplier != null) {
            inferenceResults = inferenceResultsSupplier.get();
            return this;
        }

        Map<String, Set<String>> modelsForFields = computeModelsForFields(queryRewriteContext.getIndexMetadataMap().values());
        Set<String> modelsForField = modelsForFields.get(fieldName);
        if (modelsForField == null || modelsForField.isEmpty()) {
            throw new IllegalArgumentException("Field [" + fieldName + "] is not a semantic_text field type");
        }

        if (modelsForField.size() > 1) {
            // TODO: Handle multi-index semantic queries
            throw new IllegalArgumentException("Field [" + fieldName + "] has multiple models associated with it");
        }

        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.ANY,
            modelsForField.iterator().next(),
            List.of(query),
            Map.of(),
            InputType.SEARCH
        );

        SetOnce<InferenceServiceResults> inferenceResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                InferenceAction.INSTANCE,
                inferenceRequest,
                listener.delegateFailureAndWrap((l, inferenceResponse) -> {
                    inferenceResultsSupplier.set(inferenceResponse.getResults());
                    l.onResponse(null);
                })
            )
        );

        return new SemanticQueryBuilder(this, inferenceResultsSupplier);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (inferenceResults == null) {
            throw new IllegalStateException("Query builder must be rewritten first");
        }

        List<? extends InferenceResults> inferenceResultsList = inferenceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            throw new IllegalArgumentException("No inference results retrieved for field [" + fieldName + "]");
        } else if (inferenceResultsList.size() > 1) {
            // TODO: How to handle multiple inference results?
            throw new IllegalArgumentException(inferenceResultsList.size() + " inference results retrieved for field [" + fieldName + "]");
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType == false) {
            // TODO: Better exception type to throw here?
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] is not registered as a " + SemanticTextFieldMapper.CONTENT_TYPE + " field type"
            );
        }

        return semanticQuery(inferenceResults, context);
    }

    private Map<String, Set<String>> computeModelsForFields(Collection<IndexMetadata> indexMetadataCollection) {
        Map<String, Set<String>> modelsForFields = new HashMap<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            Map<String, Set<String>> fieldsForModels = indexMetadata.getFieldsForModels();
            for (Map.Entry<String, Set<String>> entry : fieldsForModels.entrySet()) {
                for (String fieldName : entry.getValue()) {
                    Set<String> models = modelsForFields.computeIfAbsent(fieldName, v -> new HashSet<>());
                    models.add(entry.getKey());
                }
            }
        }

        return modelsForFields;
    }

    private Query semanticQuery(InferenceResults inferenceResults, SearchExecutionContext context) {
        // Cant use QueryBuilders.boolQuery() because a mapper is not registered for <field>.inference, causing
        // TermQueryBuilder#doToQuery to fail
        String inferenceResultsFieldName = fieldName + "." + INFERENCE_CHUNKS_RESULTS;
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);

        // TODO: Support dense vectors
        if (inferenceResults instanceof TextExpansionResults textExpansionResults) {
            for (TextExpansionResults.WeightedToken weightedToken : textExpansionResults.getWeightedTokens()) {
                queryBuilder.add(
                    new BoostQuery(new TermQuery(new Term(inferenceResultsFieldName, weightedToken.token())), weightedToken.weight()),
                    BooleanClause.Occur.SHOULD
                );
            }
        } else {
            throw new IllegalArgumentException("Unsupported inference results type [" + inferenceResults.getWriteableName() + "]");
        }

        BitSetProducer parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
        Query query = new ESToParentBlockJoinQuery(queryBuilder.build(), parentFilter, ScoreMode.Total, fieldName);
        return handleBoostAndQueryName(query, context);
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(inferenceResults, other.inferenceResults)
            && inferenceResultsSuppliersEqual(inferenceResultsSupplier, other.inferenceResultsSupplier);
    }

    /**
     * SetOnce does not implement equals, so use this method to determine if two inference results suppliers contain the same results.
     *
     * @param thisSupplier The supplier for this instance
     * @param otherSupplier The supplier for the other instance
     * @return True if the suppliers contain the same results
     */
    private boolean inferenceResultsSuppliersEqual(
        SetOnce<InferenceServiceResults> thisSupplier,
        SetOnce<InferenceServiceResults> otherSupplier
    ) {
        if (thisSupplier == otherSupplier) {
            return true;
        }

        InferenceServiceResults thisResults = null;
        InferenceServiceResults otherResults = null;
        if (thisSupplier != null) {
            thisResults = thisSupplier.get();
        }
        if (otherSupplier != null) {
            otherResults = otherSupplier.get();
        }

        return Objects.equals(thisResults, otherResults);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResults, inferenceResultsSupplier != null ? inferenceResultsSupplier.get() : null);
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                for (token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            query = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] no field name specified");
        }
        if (query == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] no query specified");
        }

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(fieldName, query);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
