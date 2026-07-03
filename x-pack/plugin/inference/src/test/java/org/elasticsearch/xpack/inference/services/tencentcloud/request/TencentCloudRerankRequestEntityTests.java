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
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class TencentCloudRerankRequestEntityTests extends ESTestCase {

    public void testXContent_WithRequestLevelParams_TakePrecedenceOverTaskSettings() throws IOException {
        var model = createRerankModel("bge-reranker-v2-m3", new TencentCloudRerankTaskSettings(2, false));
        var entity = new TencentCloudRerankRequestEntity("query", List.of("doc1", "doc2"), Boolean.TRUE, 5, model);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("""
            {"model":"bge-reranker-v2-m3","query":"query","documents":["doc1","doc2"],"top_n":5,"return_documents":true}"""));
    }

    public void testXContent_FallbackToTaskSettings() throws IOException {
        var model = createRerankModel("bge-reranker-v2-m3", new TencentCloudRerankTaskSettings(3, true));
        var entity = new TencentCloudRerankRequestEntity("what is ai", List.of("a", "b", "c"), null, null, model);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("""
            {"model":"bge-reranker-v2-m3","query":"what is ai","documents":["a","b","c"],"top_n":3,"return_documents":true}"""));
    }

    public void testXContent_OnlyRequiredFields() throws IOException {
        var model = createRerankModel("bge-reranker-large", TencentCloudRerankTaskSettings.EMPTY_SETTINGS);
        var entity = new TencentCloudRerankRequestEntity("q", List.of("d"), null, null, model);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("""
            {"model":"bge-reranker-large","query":"q","documents":["d"]}"""));
    }

    private static TencentCloudRerankModel createRerankModel(String modelId, TencentCloudRerankTaskSettings taskSettings) {
        var commonSettings = new TencentCloudCommonServiceSettings(modelId, null, new RateLimitSettings(20));
        var serviceSettings = new TencentCloudRerankServiceSettings(commonSettings);
        return new TencentCloudRerankModel(
            "test-inference-id",
            serviceSettings,
            taskSettings,
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
