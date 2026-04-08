/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.TOP_N_FIELD;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankModelTests extends ESTestCase {

    public static final String INFERENCE_ENTITY_ID = "inference_entity_id";

    private static final boolean TEST_RETURN_DOCUMENTS = true;
    private static final boolean ORIGINAL_RETURN_DOCUMENTS = false;
    private static final boolean NEW_RETURN_DOCUMENTS = true;

    private static final int TEST_TOP_N = 5;
    private static final int ORIGINAL_TOP_N = 10;
    private static final int NEW_TOP_N = 15;

    private static final String TEST_INSTRUCTION = "some instruction";
    private static final String ORIGINAL_INSTRUCTION = "original instruction";
    private static final String NEW_INSTRUCTION = "new instruction";

    private static final String TEST_MODEL_ID = "some_model_id";
    private static final String TEST_API_KEY = "some_api_key";
    private static final String TEST_URL = "http://www.abc.com";
    private static final String INVALID_URL = "^^^";
    private static final String DEFAULT_URL = "https://api.contextual.ai/v1/rerank";

    private static final long TEST_RATE_LIMIT = 500L;

    public void testCreateModel_NoUrl_DefaultUrl() {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            new HashMap<>(),
            new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, TEST_API_KEY)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model.getServiceSettings().uri().toString(), is(DEFAULT_URL));
    }

    public void testCreateModel_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> new ContextualAiRerankModel(
                INFERENCE_ENTITY_ID,
                new ContextualAiRerankServiceSettings(
                    new ContextualAiServiceSettings.CommonSettings(
                        ServiceUtils.createUri(INVALID_URL),
                        TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    )
                ),
                new ContextualAiRerankTaskSettings(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION),
                new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("unable to parse url [%s]. Reason: Illegal character in path", INVALID_URL))
        );
    }

    public void testOf_RequestOverridesReturnDocuments_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>(Map.of(RETURN_DOCUMENTS_FIELD, NEW_RETURN_DOCUMENTS)));

        assertThat(mergedModel, is(buildModel(NEW_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION)));
    }

    public void testOf_RequestOverridesTopN_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>(Map.of(TOP_N_FIELD, NEW_TOP_N)));

        assertThat(mergedModel, is(buildModel(ORIGINAL_RETURN_DOCUMENTS, NEW_TOP_N, ORIGINAL_INSTRUCTION)));
    }

    public void testOf_RequestOverridesInstruction_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>(Map.of(INSTRUCTION_FIELD, NEW_INSTRUCTION)));

        assertThat(mergedModel, is(buildModel(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, NEW_INSTRUCTION)));
    }

    public void testOf_EmptyTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(ORIGINAL_RETURN_DOCUMENTS, ORIGINAL_TOP_N, ORIGINAL_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>());

        assertThat(mergedModel, sameInstance(originalModel));
    }

    private static ContextualAiRerankModel buildModel(boolean returnDocuments, int topN, String instruction) {
        return new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(
                    ServiceUtils.createUri(TEST_URL),
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            ),
            new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction),
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
    }
}
