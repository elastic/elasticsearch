/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionVisitor;

import java.util.Map;

public abstract class OpenAiModel extends Model {

    public OpenAiModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    public abstract ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings);
}
