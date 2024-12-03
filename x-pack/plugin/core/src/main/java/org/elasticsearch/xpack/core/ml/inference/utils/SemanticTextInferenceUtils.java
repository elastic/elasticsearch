/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.utils;

import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;

import java.util.List;

public final class SemanticTextInferenceUtils {
    private SemanticTextInferenceUtils() {}

    public static InferenceResults validateAndConvertInferenceResults(InferenceServiceResults inferenceServiceResults, String fieldName) {
        if (inferenceServiceResults == null) {
            return null;
        }

        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            throw new IllegalArgumentException("No inference results retrieved for field [" + fieldName + "]");
        } else if (inferenceResultsList.size() > 1) {
            // The inference call should truncate if the query is too large.
            // Thus, if we receive more than one inference result, it is a server-side error.
            throw new IllegalStateException(inferenceResultsList.size() + " inference results retrieved for field [" + fieldName + "]");
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        if (inferenceResults instanceof ErrorInferenceResults errorInferenceResults) {
            throw new IllegalStateException(
                "Field [" + fieldName + "] query inference error: " + errorInferenceResults.getException().getMessage(),
                errorInferenceResults.getException()
            );
        } else if (inferenceResults instanceof WarningInferenceResults warningInferenceResults) {
            throw new IllegalStateException("Field [" + fieldName + "] query inference warning: " + warningInferenceResults.getWarning());
        } else if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlTextEmbeddingResults == false) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlTextEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint configuration changed?"
                );
            }

        return inferenceResults;
    }
}
