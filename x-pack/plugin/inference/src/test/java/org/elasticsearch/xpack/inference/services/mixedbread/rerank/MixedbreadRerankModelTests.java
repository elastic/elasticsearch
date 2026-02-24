/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadRerankModelTests extends ESTestCase {

    public static final String DEFAULT_URL = "https://api.mixedbread.com/v1/reranking";
    public static final String CUSTOM_URL = "https://custom.url.com/v1/rerank";
    public static final String MODEL_ID = "model_id_value";
    public static final String API_KEY = "secret";

    public void testConstructor_usesDefaultUrlWhenNull() {
        var model = createModel(MODEL_ID, API_KEY, null, null, null);
        assertThat(model.uri().toString(), is(DEFAULT_URL));
    }

    public void testConstructor_usesUrlWhenSpecified() {
        var model = createModel(MODEL_ID, API_KEY, null, null, CUSTOM_URL);
        assertThat(model.uri().toString(), is(CUSTOM_URL));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(MODEL_ID, API_KEY, 10, true, CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, Map.of());
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(MODEL_ID, API_KEY, 10, true, CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEqual() {
        var topN = randomNonNegativeInt();
        var returnDocuments = randomBoolean();
        var model = createModel(MODEL_ID, API_KEY, topN, returnDocuments, CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(topN, returnDocuments));
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_SetsTopN_FromRequestTaskSettings_OverridingStoredTaskSettings() {
        var model = createModel(MODEL_ID, API_KEY, 15, null, CUSTOM_URL);
        var topNFromRequest = 10;
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(topNFromRequest, null));
        var expectedModel = createModel(MODEL_ID, API_KEY, topNFromRequest, null, CUSTOM_URL);
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOf_SetsReturnDocuments_FromRequestTaskSettings() {
        var topN = 15;
        var model = createModel(MODEL_ID, API_KEY, topN, true, CUSTOM_URL);
        var returnDocumentsFromRequest = false;
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(null, returnDocumentsFromRequest));
        var expectedModel = createModel(MODEL_ID, API_KEY, topN, returnDocumentsFromRequest, CUSTOM_URL);
        assertThat(overriddenModel, is(expectedModel));
    }

    public static MixedbreadRerankModel createModel(
        String model,
        String apiKey,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable String uri
    ) {
        return new MixedbreadRerankModel(
            model,
            new MixedbreadRerankServiceSettings(model, null),
            new MixedbreadRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            uri
        );
    }
}
