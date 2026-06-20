/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings.TOP_N;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenShiftAiRerankModelTests extends ESTestCase {

    private static final String URL_VALUE = "http://www.abc.com";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String MODEL_VALUE = "some_model";
    private static final int TOP_N_VALUE = 4;
    private static final boolean RETURN_DOCUMENTS_VALUE = false;

    public static OpenShiftAiRerankModel createModel(
        String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer topN,
        @Nullable Boolean doReturnDocuments
    ) {
        return new OpenShiftAiRerankModel(
            "inferenceEntityId",
            TaskType.RERANK,
            "service",
            new OpenShiftAiRerankServiceSettings(modelId, url, null),
            new OpenShiftAiRerankTaskSettings(topN, doReturnDocuments),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testOverrideWith_SameParams_KeepsSameModel() {
        testOverrideWith_KeepsSameModel(buildTaskSettingsMap(2, true));
    }

    public void testOverrideWith_EmptyParams_KeepsSameModel() {
        testOverrideWith_KeepsSameModel(buildTaskSettingsMap(null, null));
    }

    private static void testOverrideWith_KeepsSameModel(Map<String, Object> taskSettings) {
        var model = createModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE, 2, true);
        var overriddenModel = OpenShiftAiRerankModel.of(model, taskSettings);
        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverrideWith_DifferentParams_OverridesAllTaskSettings() {
        testOverrideWith_DifferentParams(buildTaskSettingsMap(TOP_N_VALUE, RETURN_DOCUMENTS_VALUE), TOP_N_VALUE, RETURN_DOCUMENTS_VALUE);
    }

    public void testOverrideWith_DifferentParams_OverridesOnlyReturnDocuments() {
        testOverrideWith_DifferentParams(buildTaskSettingsMap(null, RETURN_DOCUMENTS_VALUE), 2, RETURN_DOCUMENTS_VALUE);
    }

    public void testOverrideWith_DifferentParams_OverridesOnlyTopN() {
        testOverrideWith_DifferentParams(buildTaskSettingsMap(TOP_N_VALUE, null), TOP_N_VALUE, true);
    }

    public void testOverrideWith_DifferentParams_OverridesNullValues() {
        var model = createModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE, null, null);
        var overriddenModel = OpenShiftAiRerankModel.of(model, buildTaskSettingsMap(TOP_N_VALUE, RETURN_DOCUMENTS_VALUE));

        assertThat(overriddenModel.getTaskSettings().getTopN(), is(TOP_N_VALUE));
        assertThat(overriddenModel.getTaskSettings().getReturnDocuments(), is(RETURN_DOCUMENTS_VALUE));
    }

    private static void testOverrideWith_DifferentParams(
        Map<String, Object> taskSettings,
        int expectedTopN,
        boolean expectedReturnDocuments
    ) {
        var model = createModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE, 2, true);
        var overriddenModel = OpenShiftAiRerankModel.of(model, taskSettings);

        assertThat(overriddenModel.getTaskSettings().getTopN(), is(expectedTopN));
        assertThat(overriddenModel.getTaskSettings().getReturnDocuments(), is(expectedReturnDocuments));
    }

    public static Map<String, Object> buildTaskSettingsMap(@Nullable Integer topN, @Nullable Boolean returnDocuments) {
        final var map = new HashMap<String, Object>();

        if (returnDocuments != null) {
            map.put(RETURN_DOCUMENTS, returnDocuments);
        }

        if (topN != null) {
            map.put(TOP_N, topN);
        }

        return map;
    }
}
