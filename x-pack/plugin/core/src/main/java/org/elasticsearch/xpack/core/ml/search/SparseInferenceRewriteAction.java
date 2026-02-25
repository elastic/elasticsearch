/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryRewriteAsyncAction;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SparseInferenceRewriteAction extends QueryRewriteAsyncAction<TextExpansionResults, SparseInferenceRewriteAction> {
    private final String inferenceId;
    private final String query;

    SparseInferenceRewriteAction(String inferenceId, String query) {
        this.inferenceId = inferenceId;
        this.query = query;
    }

    @Override
    protected void execute(Client client, ActionListener<TextExpansionResults> responseListener) {
        // TODO: Move this class to `server` and update to use InferenceAction.Request
        CoordinatedInferenceAction.Request inferRequest = CoordinatedInferenceAction.Request.forTextInput(
            inferenceId,
            List.of(query),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            false,
            null
        );

        inferRequest.setHighPriority(true);
        inferRequest.setPrefixType(TrainedModelPrefixStrings.PrefixType.SEARCH);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            CoordinatedInferenceAction.INSTANCE,
            inferRequest,
            responseListener.delegateFailureAndWrap((listener, inferenceResponse) -> {
                List<InferenceResults> inferenceResults = inferenceResponse.getInferenceResults();
                if (inferenceResults.isEmpty()) {
                    listener.onFailure(new IllegalStateException("inference response contain no results"));
                    return;
                }
                if (inferenceResults.size() > 1) {
                    listener.onFailure(new IllegalStateException("inference response should contain only one result"));
                    return;
                }

                if (inferenceResults.getFirst() instanceof TextExpansionResults textExpansionResults) {
                    listener.onResponse(textExpansionResults);
                } else if (inferenceResults.getFirst() instanceof WarningInferenceResults warning) {
                    listener.onFailure(new IllegalStateException(warning.getWarning()));
                } else {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "expected a result of type ["
                                + TextExpansionResults.NAME
                                + "] received ["
                                + inferenceResults.getFirst().getWriteableName()
                                + "]. Is ["
                                + inferenceId
                                + "] a compatible model?"
                        )
                    );
                }
            })
        );
    }

    @Override
    public int doHashCode() {
        return Objects.hash(inferenceId, query);
    }

    @Override
    public boolean doEquals(SparseInferenceRewriteAction other) {
        return Objects.equals(inferenceId, other.inferenceId) && Objects.equals(query, other.query);
    }
}
