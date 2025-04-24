/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public abstract class BulkInferenceOutputBuilder<InferenceResults extends InferenceServiceResults, OutputType>
    implements
        CheckedConsumer<InferenceAction.Response, Exception>,
        Releasable {
    protected abstract Class<InferenceResults> inferenceResultsClass();

    protected abstract OutputType buildOutput();

    public abstract void onInferenceResults(InferenceResults results);

    @Override
    public void accept(InferenceAction.Response response) throws Exception {
        InferenceServiceResults results = response.getResults();
        if (inferenceResultsClass().isInstance(response.getResults()) == false) {
            throw new IllegalStateException(
                format(
                    "Inference result has wrong type. Got [{}] while expecting [{}]",
                    results.getClass().getName(),
                    inferenceResultsClass().getName()
                )
            );
        }

        onInferenceResults(inferenceResultsClass().cast(results));
    }
}
