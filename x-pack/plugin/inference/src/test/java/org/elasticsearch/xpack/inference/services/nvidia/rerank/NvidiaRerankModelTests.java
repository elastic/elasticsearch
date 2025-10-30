/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class NvidiaRerankModelTests extends ESTestCase {

    public static NvidiaRerankModel createModel(@Nullable String url, String apiKey, @Nullable String modelId) {
        return new NvidiaRerankModel(
            "inferenceEntityId",
            TaskType.RERANK,
            "service",
            new NvidiaRerankServiceSettings(modelId, url, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
