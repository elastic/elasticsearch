/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

public class VoyageAIRerankModelTests {
    public static VoyageAIRerankModel createModel(String apiKey, String modelId, @Nullable Integer topK, @Nullable Boolean truncation) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            ESTestCase.randomAlphaOfLength(10),
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, null, truncation),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static VoyageAIRerankModel createModel(String apiKey, String modelId, @Nullable Integer topK) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            ESTestCase.randomAlphaOfLength(10),
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static VoyageAIRerankModel createModel(String modelId, @Nullable Integer topK) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            ESTestCase.randomAlphaOfLength(10),
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, null, null),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(String modelId, @Nullable Integer topK, Boolean returnDocuments, Boolean truncation) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            ESTestCase.randomAlphaOfLength(10),
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, returnDocuments, truncation),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(
        String url,
        String modelId,
        @Nullable Integer topK,
        Boolean returnDocuments,
        Boolean truncation
    ) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            url,
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, returnDocuments, truncation),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(
        String url,
        String apiKey,
        String modelId,
        @Nullable Integer topK,
        Boolean returnDocuments,
        Boolean truncation
    ) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            url,
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(modelId, null)),
            new VoyageAIRerankTaskSettings(topK, returnDocuments, truncation),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
