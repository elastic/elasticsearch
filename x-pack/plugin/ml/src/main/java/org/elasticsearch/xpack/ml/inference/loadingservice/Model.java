/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ml.inference.action.InferenceResults;

import java.util.Map;

public interface Model {

    String getResultsType();

    void infer(Map<String, Object> fields, ActionListener<InferenceResults<?>> listener);

    void classificationProbability(Map<String, Object> fields, int topN, ActionListener<InferenceResults<?>> listener);
}
