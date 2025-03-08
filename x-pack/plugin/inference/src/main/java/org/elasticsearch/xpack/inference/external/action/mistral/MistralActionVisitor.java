/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.mistral;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.util.Map;

public interface MistralActionVisitor {
    ExecutableAction create(MistralEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings);
}
