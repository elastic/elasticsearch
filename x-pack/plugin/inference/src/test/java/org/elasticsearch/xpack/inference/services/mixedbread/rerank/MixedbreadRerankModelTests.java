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
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadRerankModelTests extends ESTestCase {
    public void testConstructor_usesDefaultUrlWhenNull() {
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, null, null, null);
        assertThat(model.getServiceSettings().uri().toString(), is(TestUtils.DEFAULT_RERANK_URL));
    }

    public void testConstructor_usesUrlWhenSpecified() {
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, null, null, TestUtils.CUSTOM_URL);
        assertThat(model.getServiceSettings().uri().toString(), is(TestUtils.CUSTOM_URL));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, 10, true, TestUtils.CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, Map.of());
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, 10, true, TestUtils.CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEqual() {
        var topN = randomNonNegativeInt();
        var returnDocuments = randomBoolean();
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, topN, returnDocuments, TestUtils.CUSTOM_URL);
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(topN, returnDocuments));
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_SetsTopN_FromRequestTaskSettings_OverridingStoredTaskSettings() {
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, 15, null, TestUtils.CUSTOM_URL);
        var topNFromRequest = 10;
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(topNFromRequest, null));
        var expectedModel = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, topNFromRequest, null, TestUtils.CUSTOM_URL);
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOf_SetsReturnDocuments_FromRequestTaskSettings() {
        var topN = 15;
        var model = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, topN, true, TestUtils.CUSTOM_URL);
        var returnDocumentsFromRequest = false;
        var overriddenModel = MixedbreadRerankModel.of(model, getTaskSettingsMap(null, returnDocumentsFromRequest));
        var expectedModel = createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, topN, returnDocumentsFromRequest, TestUtils.CUSTOM_URL);
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
            new MixedbreadRerankServiceSettings(model, uri, null),
            new MixedbreadRerankTaskSettings(topN, returnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
