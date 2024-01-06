/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.huggingface.HuggingFaceActionVisitor;

import java.net.URI;

public abstract class HuggingFaceModel extends Model {
    public HuggingFaceModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    public abstract ExecutableAction accept(HuggingFaceActionVisitor creator);

    public abstract URI getUri();

    public abstract SecureString getApiKey();

    public abstract Integer getTokenLimit();
}
