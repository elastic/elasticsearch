/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiService;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiChatCompletionModelTests extends ESTestCase {

    public void testUri_IsDerivedFromTheRegion() {
        var model = createChatCompletionModel(null, "meta.llama-3.3-70b-instruct");

        assertThat(model.uri(), is(URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/chat")));
        assertThat(model.getServiceSettings().apiFormat(), is(OciGenAiChatApiFormat.GENERIC));
    }

    public void testOf_ReturnsSameModel_WhenRequestHasNoModel() {
        var model = createChatCompletionModel(null, "meta.llama-3.3-70b-instruct");

        assertThat(OciGenAiChatCompletionModel.of(model, requestBody(null)), sameInstance(model));
        assertThat(OciGenAiChatCompletionModel.of(model, requestBody("meta.llama-3.3-70b-instruct")), sameInstance(model));
    }

    public void testOf_OverridesTheModelId() {
        var model = createChatCompletionModel(null, "meta.llama-3.3-70b-instruct");

        var overridden = OciGenAiChatCompletionModel.of(model, requestBody("cohere.command-a-03-2025"));

        assertThat(overridden.getServiceSettings().modelId(), is("cohere.command-a-03-2025"));
        assertThat(overridden.getServiceSettings().apiFormat(), is(OciGenAiChatApiFormat.COHERE));
        assertThat(overridden.getServiceSettings().compartmentId(), is(COMPARTMENT_ID));
        assertThat(overridden.uri(), is(model.uri()));
        assertThat(overridden.getSecretSettings(), is(model.getSecretSettings()));
    }

    private static UnifiedCompletionRequestBody requestBody(@Nullable String modelId) {
        return new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "user", null, null, null, null)),
            modelId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    public static OciGenAiChatCompletionModel createChatCompletionModel(@Nullable String url, String modelId) {
        return createModel(url, modelId, TaskType.CHAT_COMPLETION);
    }

    public static OciGenAiChatCompletionModel createCompletionModel(@Nullable String url, String modelId) {
        return createModel(url, modelId, TaskType.COMPLETION);
    }

    public static OciGenAiChatCompletionModel createModel(@Nullable String url, String modelId, TaskType taskType) {
        return new OciGenAiChatCompletionModel(
            "id",
            taskType,
            OciGenAiService.NAME,
            url,
            new OciGenAiChatCompletionServiceSettings(
                OciGenAiTestUtils.commonSettings(REGION_VALUE, COMPARTMENT_ID, modelId, null, null, new RateLimitSettings(1000))
            ),
            OciGenAiTestUtils.createSecretSettings(),
            OciGenAiTestUtils.fixedAuthHeader()
        );
    }
}
