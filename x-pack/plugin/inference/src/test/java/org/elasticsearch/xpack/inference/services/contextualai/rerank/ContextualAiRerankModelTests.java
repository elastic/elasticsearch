/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.DEFAULT_RERANK_URL;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings.TOP_N_FIELD;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankModelTests extends ESTestCase {

    public void testCreateModel_FromMap_NoUrl_DefaultUrl() {
        var model = new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            new HashMap<>(),
            null,
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model.uri().toString(), is(DEFAULT_RERANK_URL));
    }

    public void testOf_RequestOverridesReturnDocuments_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            new HashMap<>(Map.of(RETURN_DOCUMENTS_FIELD, NEW_TEST_RETURN_DOCUMENTS))
        );

        assertThat(mergedModel, is(buildModel(NEW_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesTopN_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>(Map.of(TOP_N_FIELD, NEW_TEST_TOP_N)));

        assertThat(mergedModel, is(buildModel(INITIAL_TEST_RETURN_DOCUMENTS, NEW_TEST_TOP_N, INITIAL_TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesInstruction_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>(Map.of(INSTRUCTION_FIELD, NEW_TEST_INSTRUCTION)));

        assertThat(mergedModel, is(buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, NEW_TEST_INSTRUCTION)));
    }

    public void testOf_EmptyTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>());

        assertThat(mergedModel, sameInstance(originalModel));
    }

    private static ContextualAiRerankModel buildModel(boolean returnDocuments, int topN, String instruction) {
        return new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
            ),
            new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction),
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
    }
}
