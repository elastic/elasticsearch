/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.is;

public class NvidiaRerankModelTests extends ESTestCase {

    private static final String MODEL_VALUE = "some_model";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_INVALID_VALUE = "^^^";
    private static final String URL_DEFAULT_VALUE = "https://ai.api.nvidia.com/v1/retrieval/nvidia/reranking";

    public static NvidiaRerankModel createRerankModel(@Nullable String url, String apiKey, @Nullable String modelId) {
        return new NvidiaRerankModel(
            "inferenceEntityId",
            TaskType.RERANK,
            "service",
            new NvidiaRerankServiceSettings(modelId, url, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testCreateModel_NoModelId_ThrowsException() {
        expectThrows(NullPointerException.class, () -> createRerankModel(URL_VALUE, API_KEY_VALUE, null));
    }

    public void testCreateModel_NoUrl_DefaultUrl() {
        var model = createRerankModel(null, API_KEY_VALUE, MODEL_VALUE);

        assertThat(model.getServiceSettings().uri().toString(), is(URL_DEFAULT_VALUE));
    }

    public void testCreateModel_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createRerankModel(URL_INVALID_VALUE, API_KEY_VALUE, MODEL_VALUE)
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("unable to parse url [%s]. Reason: Illegal character in path", URL_INVALID_VALUE))
        );
    }

}
