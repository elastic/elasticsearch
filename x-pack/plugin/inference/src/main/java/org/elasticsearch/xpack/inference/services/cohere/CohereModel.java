/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.cohere.CohereActionVisitor;

import java.util.Map;

public abstract class CohereModel extends Model {
    public CohereModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected CohereModel(CohereModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    protected CohereModel(CohereModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public abstract ExecutableAction accept(CohereActionVisitor creator, Map<String, Object> taskSettings, InputType inputType);
}
