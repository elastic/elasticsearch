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
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class VoyageAIRerankModelTests {

    public static VoyageAIRerankModel createModel(String apiKey, String modelId, @Nullable Integer topN) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(ESTestCase.randomAlphaOfLength(10), modelId, null)),
            new VoyageAIRerankTaskSettings(topN, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static VoyageAIRerankModel createModel(String modelId, @Nullable Integer topN) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(ESTestCase.randomAlphaOfLength(10), modelId, null)),
            new VoyageAIRerankTaskSettings(topN, null),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(String modelId, @Nullable Integer topN, Boolean returnDocuments) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(ESTestCase.randomAlphaOfLength(10), modelId, null)),
            new VoyageAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(String url, String modelId, @Nullable Integer topN, Boolean returnDocuments) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(url, modelId, null)),
            new VoyageAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8))
        );
    }

    public static VoyageAIRerankModel createModel(
        String url,
        String apiKey,
        String modelId,
        @Nullable Integer topN,
        Boolean returnDocuments
    ) {
        return new VoyageAIRerankModel(
            "id",
            "service",
            new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(url, modelId, null)),
            new VoyageAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
