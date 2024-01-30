/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.action.ActionListener;

import java.util.List;
import java.util.Map;

public interface ModelRegistry {
    void getModel(String modelId, ActionListener<UnparsedModel> listener);

    void getModelsByTaskType(TaskType taskType, ActionListener<List<UnparsedModel>> listener);

    void getAllModels(ActionListener<List<UnparsedModel>> listener);

    void storeModel(Model model, ActionListener<Boolean> listener);

    void deleteModel(String modelId, ActionListener<Boolean> listener);

    /**
     * Semi parsed model where model id, task type and service
     * are known but the settings are not parsed.
     */
    record UnparsedModel(String modelId, TaskType taskType, String service, Map<String, Object> settings, Map<String, Object> secrets) {}
}
