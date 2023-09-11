/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class QueryRulesInferenceService {

    private static final Logger logger = LogManager.getLogger(QueryRulesInferenceService.class);
    private final Client clientWithOrigin;

    public QueryRulesInferenceService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    public boolean findInferenceRuleMatches(String modelId, String queryString, float threshold) {

        SetOnce<Boolean> inferenceRuleMatches = new SetOnce<>();

        InferModelAction.Request request = InferModelAction.Request.forTextInput(
            modelId,
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            List.of(queryString)
        );
        request.setHighPriority(true);

        clientWithOrigin.execute(InferModelAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(InferModelAction.Response response) {
                List<InferenceResults> results = response.getInferenceResults();
                for (InferenceResults result : results) {
                    @SuppressWarnings("unchecked")
                    Map<String,Float> predictedValues = (Map<String,Float>) result.asMap().get(result.getResultsField());
                    if (predictedValues.containsKey(queryString)) {
                        Float predictedValue = predictedValues.get(queryString);
                        if (predictedValue >= threshold) {
                            inferenceRuleMatches.set(true);
                            break;
                        }
                    }
                }
                inferenceRuleMatches.trySet(false);
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });

        Boolean returnValue = inferenceRuleMatches.get();
        while (returnValue == null) {
            returnValue = inferenceRuleMatches.get();
        }
        return returnValue;
    }




}
