/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class TencentCloudRerankModelTests extends ESTestCase {

    public void testUri_UsesDefaultWhenNoOverride() {
        var model = createModel(new TencentCloudCommonServiceSettings("bge-reranker-v2-m3", null, new RateLimitSettings(20)));
        assertThat(model.uri().toString(), is("http://bj.aisearch.tencentelasticsearch.com/v1/rerank"));
    }

    public void testUri_UsesOverrideWhenProvided() {
        var override = URI.create("http://custom.example.com/v1/rerank");
        var model = createModel(new TencentCloudCommonServiceSettings("bge-reranker-large", override, new RateLimitSettings(20)));
        assertThat(model.uri(), is(override));
    }

    public void testOf_EmptyOverride_ReturnsSameInstance() {
        var model = createModel(new TencentCloudCommonServiceSettings("bge-reranker-v2-m3", null, new RateLimitSettings(20)));
        var overridden = TencentCloudRerankModel.of(model, Map.of());
        assertThat(overridden, sameInstance(model));
    }

    public void testOf_MergesTaskSettings() {
        var model = createModel(new TencentCloudCommonServiceSettings("bge-reranker-v2-m3", null, new RateLimitSettings(20)));
        var overridden = TencentCloudRerankModel.of(
            model,
            new HashMap<>(Map.of(TencentCloudRerankTaskSettings.TOP_N, 5, TencentCloudRerankTaskSettings.RETURN_DOCUMENTS, true))
        );
        assertThat(overridden.getTaskSettings().getTopN(), is(5));
        assertThat(overridden.getTaskSettings().getReturnDocuments(), is(true));
    }

    private static TencentCloudRerankModel createModel(TencentCloudCommonServiceSettings commonSettings) {
        return new TencentCloudRerankModel(
            "test-inference-id",
            new TencentCloudRerankServiceSettings(commonSettings),
            TencentCloudRerankTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
