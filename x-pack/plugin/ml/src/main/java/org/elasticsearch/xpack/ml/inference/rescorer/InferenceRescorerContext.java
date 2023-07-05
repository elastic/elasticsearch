/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.util.ArrayList;
import java.util.List;

public class InferenceRescorerContext extends RescoreContext {

    final SearchExecutionContext executionContext;
    final LocalModel inferenceDefinition;

    /**
     * @param windowSize how many documents to rescore
     * @param rescorer The rescorer to apply
     * @param inferenceDefinition The local model inference definition, may be null during certain search phases.
     * @param executionContext The local shard search context
     */
    public InferenceRescorerContext(
        int windowSize,
        Rescorer rescorer,
        LocalModel inferenceDefinition,
        SearchExecutionContext executionContext
    ) {
        super(windowSize, rescorer);
        this.executionContext = executionContext;
        this.inferenceDefinition = inferenceDefinition;
    }

    List<FeatureExtractor> buildFeatureExtractors() {
        assert this.inferenceDefinition != null;
        List<FeatureExtractor> featureExtractors = new ArrayList<>();
        if (this.inferenceDefinition.inputFields().isEmpty() == false) {
            featureExtractors.add(
                new FieldValueFeatureExtractor(new ArrayList<>(this.inferenceDefinition.inputFields()), this.executionContext)
            );
        }
        return featureExtractors;
    }
}
