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

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class JinaAIRerankModelTests extends ESTestCase {

    public void testConstructor_usesDefaultUrlWhenNull() {
        var model = createModel(null, randomAlphaOfLength(10), null, null);
        assertThat(model.uri().toString(), is("https://api.jina.ai/v1/rerank"));
    }

    public void testConstructor_usesUrlWhenSpecified() {
        String url = "some_URL";
        var model = createModel(url, randomAlphaOfLength(10), null, null);
        assertThat(model.uri().toString(), is(url));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(null, "modelName", 10, true);

        var overriddenModel = JinaAIRerankModel.of(model, Map.of());
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(null, "modelName", 10, true);

        var overriddenModel = JinaAIRerankModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEqual() {
        var topN = randomNonNegativeInt();
        var returnDocuments = randomBoolean();
        var model = createModel(null, "modelName", topN, returnDocuments);

        var overriddenModel = JinaAIRerankModel.of(model, getTaskSettingsMap(topN, returnDocuments));
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_SetsTopN_FromRequestTaskSettings_OverridingStoredTaskSettings() {
        String apiKey = "apiKey";
        String modelName = "modelName";
        var model = createModel(null, apiKey, modelName, 15, null);

        var topNFromRequest = 10;
        var overriddenModel = JinaAIRerankModel.of(model, getTaskSettingsMap(topNFromRequest, null));
        var expectedModel = createModel(null, apiKey, modelName, topNFromRequest, null);
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOf_SetsReturnDocuments_FromRequestTaskSettings() {
        String apiKey = "apiKey";
        String modelName = "modelName";
        var topN = 15;
        var model = createModel(null, apiKey, modelName, topN, true);

        var returnDocumentsFromRequest = false;
        var overriddenModel = JinaAIRerankModel.of(model, getTaskSettingsMap(null, returnDocumentsFromRequest));
        var expectedModel = createModel(null, apiKey, modelName, topN, returnDocumentsFromRequest);
        assertThat(overriddenModel, is(expectedModel));
    }

    /**
     * Returns a model using null for all non-mandatory settings
     */
    public static JinaAIRerankModel createModel(String modelName) {
        return new JinaAIRerankModel(
            "id",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(modelName, null)),
            new JinaAIRerankTaskSettings(null, null),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8)),
            null
        );
    }

    public static JinaAIRerankModel createModel(String modelName, String apiKey, @Nullable Integer topN) {
        return new JinaAIRerankModel(
            "id",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(modelName, null)),
            new JinaAIRerankTaskSettings(topN, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            null
        );
    }

    public static JinaAIRerankModel createModel(String url, String modelName, @Nullable Integer topN, @Nullable Boolean returnDocuments) {
        return new JinaAIRerankModel(
            "id",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(modelName, null)),
            new JinaAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(ESTestCase.randomSecureStringOfLength(8)),
            url
        );
    }

    public static JinaAIRerankModel createModel(
        String url,
        String apiKey,
        String modelName,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        return new JinaAIRerankModel(
            "id",
            new JinaAIRerankServiceSettings(new JinaAIServiceSettings(modelName, null)),
            new JinaAIRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            url
        );
    }

}
