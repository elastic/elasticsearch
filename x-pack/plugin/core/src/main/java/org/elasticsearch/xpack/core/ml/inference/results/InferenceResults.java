/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Map;

public interface InferenceResults extends NamedWriteable, ToXContentFragment {
    String MODEL_ID_RESULTS_FIELD = "model_id";

    static void writeResult(InferenceResults results, IngestDocument ingestDocument, String resultField, String modelId) {
        ExceptionsHelper.requireNonNull(results, "results");
        ExceptionsHelper.requireNonNull(ingestDocument, "ingestDocument");
        ExceptionsHelper.requireNonNull(resultField, "resultField");
        Map<String, Object> resultMap = results.asMap();
        resultMap.put(MODEL_ID_RESULTS_FIELD, modelId);
        if (ingestDocument.hasField(resultField)) {
            ingestDocument.appendFieldValue(resultField, resultMap);
        } else {
            ingestDocument.setFieldValue(resultField, resultMap);
        }
    }

    Map<String, Object> asMap();

    Object predictedValue();
}
