/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.services;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.results.InferenceResult;

import java.util.Map;

public interface InferenceService {

    String name();

    Model parseConfig(String modelId, TaskType taskType, Map<String, Object> config);

    void infer(String modelId, TaskType taskType, Map<String, Object> config, ActionListener<InferenceResult> listener);
}
