/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudCommonServiceSettings;

import java.net.URI;

import static org.hamcrest.Matchers.is;

public class TencentCloudEmbeddingsModelTests extends ESTestCase {

    public void testUri_UsesDefaultWhenNoOverride() {
        var settings = new TencentCloudEmbeddingsServiceSettings(
            new TencentCloudCommonServiceSettings("bge-m3", null, new RateLimitSettings(20)),
            SimilarityMeasure.DOT_PRODUCT,
            1024,
            8192
        );
        var model = createModel(settings);

        assertThat(model.uri().toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/embeddings"));
        assertThat(model.getServiceSettings().modelId(), is("bge-m3"));
    }

    public void testUri_UsesOverrideWhenProvided() {
        var override = URI.create("http://custom.example.com/embeddings");
        var settings = new TencentCloudEmbeddingsServiceSettings(
            new TencentCloudCommonServiceSettings("bge-m3", override, new RateLimitSettings(20)),
            null,
            null,
            null
        );
        var model = createModel(settings);
        assertThat(model.uri(), is(override));
    }

    public void testCopyConstructor_UpdatesServiceSettings() {
        var original = createModel(
            new TencentCloudEmbeddingsServiceSettings(
                new TencentCloudCommonServiceSettings("bge-m3", null, new RateLimitSettings(20)),
                null,
                null,
                null
            )
        );
        var updated = new TencentCloudEmbeddingsModel(
            original,
            original.getServiceSettings().updateEmbeddingDetails(1024, SimilarityMeasure.COSINE)
        );
        assertThat(updated.getServiceSettings().dimensions(), is(1024));
        assertThat(updated.getServiceSettings().similarity(), is(SimilarityMeasure.COSINE));
    }

    public static TencentCloudEmbeddingsModel createModel(TencentCloudEmbeddingsServiceSettings serviceSettings) {
        return new TencentCloudEmbeddingsModel(
            "test-inference-id",
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new org.elasticsearch.common.settings.SecureString("sk-test".toCharArray()))
        );
    }
}
