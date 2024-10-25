/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Map;
import java.util.Objects;

public interface InferenceResults extends NamedWriteable, ToXContentFragment {
    String MODEL_ID_RESULTS_FIELD = "model_id";

    static void writeResult(InferenceResults results, IngestDocument ingestDocument, String resultField, String modelId) {
        Objects.requireNonNull(results, "results");
        Objects.requireNonNull(ingestDocument, "ingestDocument");
        Objects.requireNonNull(resultField, "resultField");
        Map<String, Object> resultMap = results.asMap();
        resultMap.put(MODEL_ID_RESULTS_FIELD, modelId);
        setOrAppendValue(resultField, resultMap, ingestDocument);
    }

    static void writeResultToField(
        InferenceResults results,
        IngestDocument ingestDocument,
        @Nullable String basePath,
        String outputField,
        String modelId,
        boolean includeModelId
    ) {
        Objects.requireNonNull(results, "results");
        Objects.requireNonNull(ingestDocument, "ingestDocument");
        Objects.requireNonNull(outputField, "outputField");
        Map<String, Object> resultMap = results.asMap(outputField);
        if (includeModelId) {
            resultMap.put(MODEL_ID_RESULTS_FIELD, modelId);
        }
        if (basePath == null) {
            // insert the results into the root of the document
            for (var entry : resultMap.entrySet()) {
                setOrAppendValue(entry.getKey(), entry.getValue(), ingestDocument);
            }
        } else {
            for (var entry : resultMap.entrySet()) {
                setOrAppendValue(basePath + "." + entry.getKey(), entry.getValue(), ingestDocument);
            }
        }
    }

    private static void setOrAppendValue(String path, Object value, IngestDocument ingestDocument) {
        if (ingestDocument.hasField(path)) {
            ingestDocument.appendFieldValue(path, value);
        } else {
            ingestDocument.setFieldValue(path, value);
        }
    }

    String getResultsField();

    /**
     * Convert to a map
     * @return Map representation of the InferenceResult
     */
    Map<String, Object> asMap();

    /**
     * Convert to a map placing the inference result in {@code outputField}
     * @param outputField Write the inference result to this field
     * @return Map representation of the InferenceResult
     */
    Map<String, Object> asMap(String outputField);

    Object predictedValue();
}
