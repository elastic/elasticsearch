/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.net.URI;

import static org.hamcrest.Matchers.is;

public class TencentCloudChatCompletionModelTests extends ESTestCase {

    public void testUri_UsesDefaultWhenNoOverride() {
        var model = createModel(new TencentCloudCommonServiceSettings("deepseek-v3", null, new RateLimitSettings(5)));
        assertThat(model.uri().toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/chat/completions"));
    }

    public void testUri_UsesOverrideWhenProvided() {
        var override = URI.create("http://custom.example.com/v1/chat/completions");
        var model = createModel(new TencentCloudCommonServiceSettings("deepseek-v3", override, new RateLimitSettings(5)));
        assertThat(model.uri(), is(override));
    }

    public void testModelIdAccessor() {
        var model = createModel(new TencentCloudCommonServiceSettings("deepseek-r1", null, new RateLimitSettings(5)));
        assertThat(model.model(), is("deepseek-r1"));
        assertThat(model.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    private static TencentCloudChatCompletionModel createModel(TencentCloudCommonServiceSettings commonSettings) {
        return new TencentCloudChatCompletionModel(
            "test-inference-id",
            TaskType.CHAT_COMPLETION,
            new TencentCloudChatCompletionServiceSettings(commonSettings),
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
