/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TencentCloudEmbeddingsModelTests extends ESTestCase {

    public void testUri_UsesDefaultWhenNoOverride() {
        var settings = new TencentCloudEmbeddingsServiceSettings(
            "bge-m3",
            null,
            new RateLimitSettings(20),
            SimilarityMeasure.DOT_PRODUCT,
            1024,
            8192
        );
        var model = createModel(settings);

        assertThat(model.uri().toString(), is("https://bj.aisearch.tencentelasticsearch.com/v1/embeddings"));
        assertThat(model.getServiceSettings().modelId(), is("bge-m3"));
    }

    public void testUri_UsesRegion() {
        var settings = new TencentCloudEmbeddingsServiceSettings("bge-m3", "sh", new RateLimitSettings(20), null, null, null);
        var model = createModel(settings);
        assertThat(model.uri().toString(), is("https://sh.aisearch.tencentelasticsearch.com/v1/embeddings"));
    }

    public void testRateLimitGroupingHash_GroupsByModelIdAndUri() {
        var settingsBj = new TencentCloudEmbeddingsServiceSettings("bge-m3", "bj", new RateLimitSettings(20), null, null, null);
        var settingsSh = new TencentCloudEmbeddingsServiceSettings("bge-m3", "sh", new RateLimitSettings(20), null, null, null);
        var settingsDifferentModel = new TencentCloudEmbeddingsServiceSettings(
            "bge-large-en",
            "bj",
            new RateLimitSettings(20),
            null,
            null,
            null
        );

        var modelBj = createModel(settingsBj);
        var modelBjCopy = createModel(settingsBj);
        var modelSh = createModel(settingsSh);
        var modelDifferentModel = createModel(settingsDifferentModel);

        // Same model id + URI → same bucket
        assertThat(modelBj.rateLimitGroupingHash(), is(modelBjCopy.rateLimitGroupingHash()));
        // Different region → different URI → different bucket
        assertThat(modelBj.rateLimitGroupingHash(), not(is(modelSh.rateLimitGroupingHash())));
        // Different model id → different bucket
        assertThat(modelBj.rateLimitGroupingHash(), not(is(modelDifferentModel.rateLimitGroupingHash())));
        // Verify the hash is based on modelId and uri
        assertThat(modelBj.rateLimitGroupingHash(), is(Objects.hash(modelBj.getServiceSettings().modelId(), modelBj.uri())));
    }

    public void testCopyConstructor_UpdatesServiceSettings() {
        var original = createModel(new TencentCloudEmbeddingsServiceSettings("bge-m3", null, new RateLimitSettings(20), null, null, null));
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
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
