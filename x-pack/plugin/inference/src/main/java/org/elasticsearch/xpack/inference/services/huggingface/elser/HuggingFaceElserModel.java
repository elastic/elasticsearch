/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettings;

public class HuggingFaceElserModel extends Model {
    public HuggingFaceElserModel(String modelId, TaskType taskType, String service, ElserMlNodeServiceSettings serviceSettings) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings));
    }
}
