/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.Map;

public class OpenAiActionCreator implements OpenAiActionVisitor {
    private final Sender sender;
    private final ThrottlerManager throttlerManager;
    private final Map<String, Object> taskSettings;

    public OpenAiActionCreator(Sender sender, ThrottlerManager throttlerManager, Map<String, Object> taskSettings) {
        this.sender = sender;
        this.throttlerManager = throttlerManager;
        this.taskSettings = taskSettings;
    }

    @Override
    public ExecutableAction create(OpenAiEmbeddingsModel model) {
        var overriddenModel = model.overrideWith(taskSettings);

        return new OpenAiEmbeddingsAction(sender, overriddenModel, throttlerManager);
    }
}
