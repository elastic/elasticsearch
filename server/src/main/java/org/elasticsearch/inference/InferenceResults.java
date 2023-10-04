/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteable;
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
        if (ingestDocument.hasField(resultField)) {
            ingestDocument.appendFieldValue(resultField, resultMap);
        } else {
            ingestDocument.setFieldValue(resultField, resultMap);
        }
    }

    String getResultsField();

    Map<String, Object> asMap();

    Object predictedValue();
}
