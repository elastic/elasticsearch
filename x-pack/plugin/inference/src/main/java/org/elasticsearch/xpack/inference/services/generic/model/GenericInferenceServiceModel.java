/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.generic.model;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.generic.GenericInferenceSecretSettings;

import java.util.Map;

public class GenericInferenceServiceModel extends Model {

    public GenericInferenceServiceModel(String inferenceEntityId, TaskType taskType, String service, Map<String, Object> modelConfig) {
        super(
            new ModelConfigurations(
                inferenceEntityId,
                taskType,
                service,
                new GenericInferenceServiceSettings(modelConfig),
                new GenericInferenceTaskSettings(modelConfig),
                ChunkingSettingsBuilder.fromMap(modelConfig)
            ),
            new ModelSecrets(new GenericInferenceSecretSettings(modelConfig))
        );
    }
}
