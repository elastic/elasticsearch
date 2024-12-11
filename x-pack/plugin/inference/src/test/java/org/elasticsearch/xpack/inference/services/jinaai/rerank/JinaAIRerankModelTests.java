/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class JinaAIRerankModelTests extends ESTestCase {

    public static JinaAIRerankModel createModel(String apiKey, @Nullable String modelId, @Nullable Integer topN) {
        return new JinaAIRerankModel(
            "id",
            "service",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(randomAlphaOfLength(10), modelId, null)),
            new JinaAIRerankTaskSettings(topN, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static JinaAIRerankModel createModel(@Nullable String modelId, @Nullable Integer topN) {
        return new JinaAIRerankModel(
            "id",
            "service",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(randomAlphaOfLength(10), modelId, null)),
            new JinaAIRerankTaskSettings(topN, null),
            new DefaultSecretSettings(randomSecureStringOfLength(8))
        );
    }

    public static JinaAIRerankModel createModel(@Nullable String modelId, @Nullable Integer topN, Boolean returnDocuments) {
        return new JinaAIRerankModel(
            "id",
            "service",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(randomAlphaOfLength(10), modelId, null)),
            new JinaAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(randomSecureStringOfLength(8))
        );
    }

    public static JinaAIRerankModel createModel(String url, @Nullable String modelId, @Nullable Integer topN, Boolean returnDocuments) {
        return new JinaAIRerankModel(
            "id",
            "service",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(randomAlphaOfLength(10), modelId, null)),
            new JinaAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(randomSecureStringOfLength(8))
        );
    }

}
