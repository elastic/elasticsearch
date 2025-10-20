/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class OpenShiftAiRerankModelTests extends ESTestCase {

    public static OpenShiftAiRerankModel createModel(String url, String apiKey, @Nullable String modelId) {
        return createModel(url, apiKey, modelId, 2, true);
    }

    public static OpenShiftAiRerankModel createModel(
        String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer topN,
        @Nullable Boolean doReturnDocuments
    ) {
        return new OpenShiftAiRerankModel(
            "inferenceEntityId",
            TaskType.RERANK,
            "service",
            new OpenShiftAiRerankServiceSettings(modelId, url, null),
            new OpenShiftAiRerankTaskSettings(topN, doReturnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
