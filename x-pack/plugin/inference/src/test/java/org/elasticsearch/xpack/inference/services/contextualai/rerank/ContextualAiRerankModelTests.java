/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_TOP_N;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankModelTests extends ESTestCase {

    public void testCreateModel_FromMap_AllFields() {
        var model = new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            ContextualAiRerankServiceSettingsTests.buildServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION),
            DefaultSecretSettingsTests.getSecretSettingsMap(TEST_API_KEY),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model, is(buildModel(TEST_RETURN_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesAllValuesInEmptyMap_AllValuesUpdated() {
        var originalModel = buildModel(null, null, null);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(NEW_TEST_RETURN_DOCUMENTS, NEW_TEST_TOP_N, NEW_TEST_INSTRUCTION)
        );

        assertThat(mergedModel, is(buildModel(NEW_TEST_RETURN_DOCUMENTS, NEW_TEST_TOP_N, NEW_TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesReturnDocuments_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(NEW_TEST_RETURN_DOCUMENTS, null, null)
        );

        assertThat(mergedModel, is(buildModel(NEW_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesTopN_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(null, NEW_TEST_TOP_N, null)
        );

        assertThat(mergedModel, is(buildModel(INITIAL_TEST_RETURN_DOCUMENTS, NEW_TEST_TOP_N, INITIAL_TEST_INSTRUCTION)));
    }

    public void testOf_RequestOverridesInstruction_KeepsOtherFieldsUnchanged() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(null, null, NEW_TEST_INSTRUCTION)
        );

        assertThat(mergedModel, is(buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, NEW_TEST_INSTRUCTION)));
    }

    public void testOf_EmptyTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(originalModel, new HashMap<>());

        assertThat(mergedModel, sameInstance(originalModel));
    }

    public void testOf_SameValuesInTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(
                INITIAL_TEST_RETURN_DOCUMENTS,
                INITIAL_TEST_TOP_N,
                INITIAL_TEST_INSTRUCTION
            )
        );

        assertThat(mergedModel, sameInstance(originalModel));
    }

    public void testOf_OnlySameReturnDocumentsInTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(INITIAL_TEST_RETURN_DOCUMENTS, null, null)
        );

        assertThat(mergedModel, sameInstance(originalModel));
    }

    public void testOf_OnlySameTopNInTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(null, INITIAL_TEST_TOP_N, null)
        );

        assertThat(mergedModel, sameInstance(originalModel));
    }

    public void testOf_OnlySameInstructionInTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var originalModel = buildModel(INITIAL_TEST_RETURN_DOCUMENTS, INITIAL_TEST_TOP_N, INITIAL_TEST_INSTRUCTION);

        var mergedModel = ContextualAiRerankModel.of(
            originalModel,
            ContextualAiRerankTaskSettingsTests.buildTaskSettingsMap(null, null, INITIAL_TEST_INSTRUCTION)
        );

        assertThat(mergedModel, sameInstance(originalModel));
    }

    private static ContextualAiRerankModel buildModel(
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        @Nullable String instruction
    ) {
        return new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
            ),
            new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction),
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
    }

    public static ContextualAiRerankModel createModel(
        String url,
        String apiKey,
        String modelId,
        @Nullable Integer topN,
        @Nullable String instruction
    ) {
        return new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(modelId, new RateLimitSettings(TEST_RATE_LIMIT))
            ),
            new ContextualAiRerankTaskSettings(null, topN, instruction),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            URI.create(url)
        );
    }
}
