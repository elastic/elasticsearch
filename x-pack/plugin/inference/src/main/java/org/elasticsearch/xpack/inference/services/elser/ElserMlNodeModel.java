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

public class ElserMlNodeModel extends Model {

    public ElserMlNodeModel(
        String modelId,
        TaskType taskType,
        String service,
        ElserMlNodeServiceSettings serviceSettings,
        ElserMlNodeTaskSettings taskSettings
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings));
    }

    @Override
    public ElserMlNodeServiceSettings getServiceSettings() {
        return (ElserMlNodeServiceSettings) super.getServiceSettings();
    }

    @Override
    public ElserMlNodeTaskSettings getTaskSettings() {
        return (ElserMlNodeTaskSettings) super.getTaskSettings();
    }
}
