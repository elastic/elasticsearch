/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class TencentCloudUtilsTests extends ESTestCase {

    public void testBuildHost_AppendsCorrectSuffix() {
        assertThat(TencentCloudUtils.buildHost("bj"), is("bj.aisearch.tencentelasticsearch.com"));
        assertThat(TencentCloudUtils.buildHost("sh"), is("sh.aisearch.tencentelasticsearch.com"));
        assertThat(TencentCloudUtils.buildHost("gz"), is("gz.aisearch.tencentelasticsearch.com"));
    }

    public void testBuildUri_Embeddings() {
        var uri = TencentCloudUtils.buildUri("bj", TencentCloudUtils.VERSION_1, TencentCloudUtils.EMBEDDINGS_PATH);
        assertThat(uri.toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/embeddings"));
    }

    public void testBuildUri_ChatCompletions() {
        var uri = TencentCloudUtils.buildUri(
            "sh",
            TencentCloudUtils.VERSION_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_1,
            TencentCloudUtils.CHAT_COMPLETIONS_PATH_2
        );
        assertThat(uri.toString(), is("https://sh.aisearch.tencentelasticsearch.com/v1/chat/completions"));
    }

    public void testBuildUri_Rerank() {
        var uri = TencentCloudUtils.buildUri("gz", TencentCloudUtils.VERSION_1, TencentCloudUtils.RERANK_PATH);
        assertThat(uri.toString(), is("https://gz.aisearch.tencentelasticsearch.com/v1/rerank"));
    }

    public void testDefaultRegion_IsBj() {
        assertThat(TencentCloudUtils.DEFAULT_REGION, is("bj"));
    }

    public void testBuildUri_DefaultRegion_ProducesExpectedEmbeddingsUri() {
        var uri = TencentCloudUtils.buildUri(
            TencentCloudUtils.DEFAULT_REGION,
            TencentCloudUtils.VERSION_1,
            TencentCloudUtils.EMBEDDINGS_PATH
        );
        assertThat(uri.toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/embeddings"));
    }
}
