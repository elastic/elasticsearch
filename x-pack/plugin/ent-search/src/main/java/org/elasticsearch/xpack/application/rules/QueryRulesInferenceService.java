/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.util.SetOnce;
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

    private static final int TIMEOUT_MS = 10000;

    private static final Logger logger = LogManager.getLogger(QueryRulesInferenceService.class);
    private final Client clientWithOrigin;

    public QueryRulesInferenceService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    public boolean findInferenceRuleMatches(String modelId, String queryString, String matchValue, float threshold) {

        SetOnce<Boolean> inferenceRuleMatches = new SetOnce<>();
        InferModelAction.Request request = makeRequest(modelId, queryString);
        InferModelAction.Response response = getInferences(request);
        List<InferenceResults> results = response.getInferenceResults();
        for (InferenceResults result : results) {
            @SuppressWarnings("unchecked")
            Map<String, Float> predictedValues = (Map<String, Float>) result.asMap().get(result.getResultsField());
            if (predictedValues.containsKey(queryString)) {
                Float predictedValue = predictedValues.get(matchValue);
                if (predictedValue >= threshold) {
                    inferenceRuleMatches.set(true);
                    break;
                }
            }
        }
        inferenceRuleMatches.trySet(false);
        return inferenceRuleMatches.get();
    }

    private InferModelAction.Request makeRequest(String modelId, String queryString) {
        InferModelAction.Request request = InferModelAction.Request.forTextInput(
            modelId,
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            List.of(queryString)
        );
        request.setHighPriority(true);
        return request;
    }

    InferModelAction.Response getInferences(InferModelAction.Request request) {
        return clientWithOrigin.execute(InferModelAction.INSTANCE, request).actionGet(TIMEOUT_MS);
    }

}
