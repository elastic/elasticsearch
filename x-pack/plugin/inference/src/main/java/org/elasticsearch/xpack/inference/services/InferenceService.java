/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;

import java.util.Map;

public interface InferenceService {

    String name();

    Model parseConfig(String modelId, TaskType taskType, Map<String, Object> config);

    void init(Model model, ActionListener<Boolean> listener);

    void infer(String modelId, TaskType taskType, String input, Map<String, Object> config, ActionListener<InferenceResult> listener);
}
