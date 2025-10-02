/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.containsString;

public class HuggingFaceRerankModelTests extends ESTestCase {

    public void testThrowsURISyntaxException_ForInvalidUrl() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> createModel("^^", "secret", "model", 8, false));
        assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
    }

    public static HuggingFaceRerankModel createModel(
        String url,
        String apiKey,
        String modelId,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        return new HuggingFaceRerankModel(
            modelId,
            TaskType.RERANK,
            "service",
            new HuggingFaceRerankServiceSettings(url),
            new HuggingFaceRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
