/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class QueryRulesInferenceService {

    public static final Set<String> SUPPORTED_INFERENCE_CONFIGS = Set.of(TextExpansionConfig.NAME, TextClassificationConfig.NAME);

    private static final int TIMEOUT_MS = 10000;

    private static final Logger logger = LogManager.getLogger(QueryRulesInferenceService.class);
    private final Client clientWithOrigin;

    public QueryRulesInferenceService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    public boolean findInferenceRuleMatches(
        String modelId,
        String inferenceConfig,
        String queryString,
        String matchValue,
        float threshold
    ) {
        if (inferenceConfig.equals(TextExpansionConfig.NAME)) {
            return findTextExpansionInferenceRuleMatches(modelId, queryString, matchValue, threshold);
        } else if (inferenceConfig.equals(TextClassificationConfig.NAME)) {
            return findTextClassificationInferenceRuleMatches(modelId, queryString, matchValue, threshold);
        } else {
            throw new UnsupportedOperationException("Only " + SUPPORTED_INFERENCE_CONFIGS + "inference configurations supported");
        }
    }

    private boolean findTextExpansionInferenceRuleMatches(String modelId, String queryString, String matchValue, float threshold) {
        InferModelAction.Request request = createRequest(modelId, TextExpansionConfigUpdate.EMPTY_UPDATE, queryString);
        List<InferenceResults> results = getInferences(request);
        for (InferenceResults result : results) {
            @SuppressWarnings("unchecked")
            Map<String, Float> predictedValues = (Map<String, Float>) result.asMap().get(result.getResultsField());
            if (predictedValues.containsKey(queryString)) {
                Float predictedValue = predictedValues.get(matchValue);
                if (predictedValue >= threshold) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean findTextClassificationInferenceRuleMatches(String modelId, String queryString, String matchValue, float threshold) {
        InferModelAction.Request request = createRequest(modelId, TextClassificationConfigUpdate.fromMap(Map.of()), queryString);
        List<InferenceResults> results = getInferences(request);
        for (InferenceResults result : results) {
            String predictedValue = result.asMap().get(result.getResultsField()).toString();
            float predictedProbability = ((Double) result.asMap().get(InferenceResults.PREDICTION_PROBABILITY)).floatValue();
            if (predictedValue.equals(matchValue) && predictedProbability >= threshold) {
                return true;
            }
        }
        return false;
    }

    private InferModelAction.Request createRequest(String modelId, InferenceConfigUpdate inferenceConfigUpdate, String queryString) {
        InferModelAction.Request request = InferModelAction.Request.forTextInput(modelId, inferenceConfigUpdate, List.of(queryString));
        request.setHighPriority(true);
        return request;
    }

    List<InferenceResults> getInferences(InferModelAction.Request request) {
        InferModelAction.Response response = clientWithOrigin.execute(InferModelAction.INSTANCE, request).actionGet(TIMEOUT_MS);
        return response.getInferenceResults();
    }

}
