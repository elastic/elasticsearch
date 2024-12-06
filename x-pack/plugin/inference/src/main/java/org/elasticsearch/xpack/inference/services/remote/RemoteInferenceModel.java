/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.remote;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultTaskSettings;

class RemoteInferenceModel extends Model {
    private final DefaultServiceSettings serviceSettings;
    private final DefaultSecretSettings secretSettings;
    private final DefaultTaskSettings taskSettings;
    private final String service;
    private final TaskType taskType;

    RemoteInferenceModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        DefaultServiceSettings serviceSettings,
        DefaultSecretSettings secretSettings,
        DefaultTaskSettings taskSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
        this.serviceSettings = serviceSettings;
        this.secretSettings = secretSettings;
        this.taskSettings = taskSettings;
        this.service = service;
        this.taskType = taskType;
    }

    public String humanReadableName() {
        return service + " " + taskType.toString();
    }

    @Override
    public DefaultServiceSettings getServiceSettings() {
        return serviceSettings;
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return secretSettings;
    }

    @Override
    public DefaultTaskSettings getTaskSettings() {
        return taskSettings;
    }
}
