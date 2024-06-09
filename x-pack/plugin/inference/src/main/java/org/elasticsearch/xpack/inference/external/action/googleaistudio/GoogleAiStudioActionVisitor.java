/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.googleaistudio;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;

import java.util.Map;

public interface GoogleAiStudioActionVisitor {

    ExecutableAction create(GoogleAiStudioCompletionModel model, Map<String, Object> taskSettings);

    ExecutableAction create(GoogleAiStudioEmbeddingsModel model, Map<String, Object> taskSettings);
}
