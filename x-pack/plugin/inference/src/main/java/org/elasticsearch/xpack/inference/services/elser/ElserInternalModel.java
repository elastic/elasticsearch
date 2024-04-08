/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;

public class ElserInternalModel extends Model {

    public ElserInternalModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ElserInternalServiceSettings serviceSettings,
        ElserMlNodeTaskSettings taskSettings
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings));
    }

    @Override
    public ElserInternalServiceSettings getServiceSettings() {
        return (ElserInternalServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElserMlNodeTaskSettings getTaskSettings() {
        return (ElserMlNodeTaskSettings) super.getTaskSettings();
    }
}
