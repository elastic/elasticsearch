/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_FIELD = new ParseField("query");

    private static final ConstructingObjectParser<SemanticQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME, false, args -> new SemanticQueryBuilder((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        declareStandardFields(PARSER);
    }

    private final String fieldName;
    private final String query;

    private SetOnce<InferenceServiceResults> inferenceResultsSupplier;
    private InferenceServiceResults inferenceResults;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + FIELD_FIELD.getPreferredName() + " value");
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
        this.inferenceResults = in.readOptionalNamedWriteable(InferenceServiceResults.class);
        if (this.inferenceResults != null) {
            // The supplier is generally not used after the results are set, but set the supplier to maintain equality when serializing
            // & deserializing
            this.inferenceResultsSupplier = new SetOnce<>(this.inferenceResults);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceResultsSupplier != null && inferenceResults == null) {
            throw new IllegalStateException("Inference results supplier is set, but inference results is null. Missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(query);
        out.writeOptionalNamedWriteable(inferenceResults);
    }

    private SemanticQueryBuilder(
        SemanticQueryBuilder other,
        SetOnce<InferenceServiceResults> inferenceResultsSupplier,
        InferenceServiceResults inferenceResults
    ) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
        this.inferenceResults = inferenceResults;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SEMANTIC_QUERY;
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        SearchExecutionContext searchExecutionContext = queryRewriteContext.convertToSearchExecutionContext();
        if (searchExecutionContext != null) {
            return doRewriteBuildSemanticQuery(searchExecutionContext);
        }

        return doRewriteGetInferenceResults(queryRewriteContext);
    }

    private QueryBuilder doRewriteBuildSemanticQuery(SearchExecutionContext searchExecutionContext) {
        if (inferenceResults == null) {
            throw new IllegalStateException("Query builder must have inference results before rewriting to another query type");
        }

        MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            List<? extends InferenceResults> inferenceResultsList = inferenceResults.transformToCoordinationFormat();
            if (inferenceResultsList.isEmpty()) {
                throw new IllegalArgumentException("No inference results retrieved for field [" + fieldName + "]");
            } else if (inferenceResultsList.size() > 1) {
                // TODO: How to handle multiple inference results?
                throw new IllegalArgumentException(
                    inferenceResultsList.size() + " inference results retrieved for field [" + fieldName + "]"
                );
            }

            InferenceResults inferenceResults = inferenceResultsList.get(0);
            return semanticTextFieldType.semanticQuery(inferenceResults, boost(), queryName());
        } else {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] of type [" + fieldType.typeName() + "] does not support " + NAME + " queries"
            );
        }
    }

    private SemanticQueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) {
        if (inferenceResults != null) {
            return this;
        }

        if (inferenceResultsSupplier != null) {
            InferenceServiceResults inferenceResults = inferenceResultsSupplier.get();
            return inferenceResults != null ? new SemanticQueryBuilder(this, inferenceResultsSupplier, inferenceResults) : this;
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalStateException(
                "Rewriting on the coordinator node requires a query rewrite context with non-null resolved indices"
            );
        } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " query does not support cross-cluster search");
        }

        String inferenceId = getInferenceIdForForField(resolvedIndices.getConcreteLocalIndicesMetadata().values(), fieldName);
        SetOnce<InferenceServiceResults> inferenceResultsSupplier;
        if (inferenceId != null) {
            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceId,
                null,
                List.of(query),
                Map.of(),
                InputType.SEARCH,
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
            );

            inferenceResultsSupplier = new SetOnce<>();
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
        } else {
            // The most likely reason for a null inference ID is an invalid field name, invalid index name(s), or a combination of both.
            // Set the inference results to an empty list so that query rewriting can continue.
            // Invalid index names will be handled in TransportSearchAction, which will throw IndexNotFoundException.
            // Invalid field names will be handled later in the rewrite process.
            inferenceResultsSupplier = new SetOnce<>(new SparseEmbeddingResults(List.of()));
        }

        return new SemanticQueryBuilder(this, inferenceResultsSupplier, null);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException(NAME + " should have been rewritten to another query type");
    }

    private static String getInferenceIdForForField(Collection<IndexMetadata> indexMetadataCollection, String fieldName) {
        String inferenceId = null;
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
            String indexInferenceId = inferenceFieldMetadata != null ? inferenceFieldMetadata.getInferenceId() : null;
            if (indexInferenceId != null) {
                if (inferenceId != null && inferenceId.equals(indexInferenceId) == false) {
                    throw new IllegalArgumentException("Field [" + fieldName + "] has multiple inference IDs associated with it");
                }

                inferenceId = indexInferenceId;
            }
        }

        return inferenceId;
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
}
