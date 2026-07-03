/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class TencentCloudEmbeddingsRequestEntityTests extends ESTestCase {

    public void testXContent_WritesModelAndInputArray() throws IOException {
        var model = createEmbeddingsModel("bge-m3");
        var entity = new TencentCloudEmbeddingsRequestEntity(List.of("hello", "world"), model);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("""
            {"model":"bge-m3","input":["hello","world"]}"""));
    }

    public void testXContent_WithSingleInput() throws IOException {
        var model = createEmbeddingsModel("bge-large-zh-v1.5");
        var entity = new TencentCloudEmbeddingsRequestEntity(List.of("你好"), model);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("""
            {"model":"bge-large-zh-v1.5","input":["你好"]}"""));
    }

    private static TencentCloudEmbeddingsModel createEmbeddingsModel(String modelId) {
        var commonSettings = new TencentCloudCommonServiceSettings(modelId, null, new RateLimitSettings(20));
        var serviceSettings = new TencentCloudEmbeddingsServiceSettings(commonSettings, null, null, null);
        return new TencentCloudEmbeddingsModel(
            "test-inference-id",
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
