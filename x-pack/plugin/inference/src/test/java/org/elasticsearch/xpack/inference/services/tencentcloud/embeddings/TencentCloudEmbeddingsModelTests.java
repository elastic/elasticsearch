/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TencentCloudEmbeddingsModelTests extends ESTestCase {

    public void testUri_UsesDefaultWhenNoOverride() {
        var settings = new TencentCloudTextEmbeddingServiceSettings(
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
        var settings = new TencentCloudTextEmbeddingServiceSettings("bge-m3", "sh", new RateLimitSettings(20), null, null, null);
        var model = createModel(settings);
        assertThat(model.uri().toString(), is("https://sh.aisearch.tencentelasticsearch.com/v1/embeddings"));
    }

    public void testRateLimitGroupingHash_GroupsByModelIdAndUri() {
        var settingsBj = new TencentCloudTextEmbeddingServiceSettings("bge-m3", "bj", new RateLimitSettings(20), null, null, null);
        var settingsSh = new TencentCloudTextEmbeddingServiceSettings("bge-m3", "sh", new RateLimitSettings(20), null, null, null);
        var settingsDifferentModel = new TencentCloudTextEmbeddingServiceSettings(
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
        var original = createModel(
            new TencentCloudTextEmbeddingServiceSettings("bge-m3", null, new RateLimitSettings(20), null, null, null)
        );
        var updated = new TencentCloudEmbeddingsModel(
            original,
            original.getServiceSettings().updateEmbeddingDetails(1024, SimilarityMeasure.COSINE)
        );
        assertThat(updated.getServiceSettings().dimensions(), is(1024));
        assertThat(updated.getServiceSettings().similarity(), is(SimilarityMeasure.COSINE));
    }

    public void testEmbeddingTask_UsesMultimodalPathAndSettings() {
        var model = new TencentCloudEmbeddingsModel(
            "test-inference-id",
            TaskType.EMBEDDING,
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "WeMM-Embedding-2B", "region", "sh")),
            new HashMap<>(Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "Document")),
            null,
            new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, "sk-test")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(model.uri().toString(), is("https://sh.aisearch.tencentelasticsearch.com/v1/multimodal-embeddings"));
        assertThat(model.getServiceSettings(), instanceOf(TencentCloudEmbeddingServiceSettings.class));
        assertTrue(model.getServiceSettings().isMultimodal());
        assertThat(model.getMultimodalTaskSettings(), is(new TencentCloudMultimodalEmbeddingsTaskSettings(null, "Document")));
    }

    public void testTextEmbeddingTask_DoesNotConsumeTaskSettings() {
        var taskSettings = new HashMap<String, Object>(Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "Document"));
        var model = new TencentCloudEmbeddingsModel(
            "test-inference-id",
            TaskType.TEXT_EMBEDDING,
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "bge-m3")),
            taskSettings,
            null,
            null,
            ConfigurationParseContext.REQUEST
        );

        assertThat(model.getServiceSettings(), instanceOf(TencentCloudTextEmbeddingServiceSettings.class));
        assertThat(model.getTaskSettings(), is(TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS));
        assertThat(taskSettings.size(), is(1));
    }

    public void testOf_AppliesRequestTaskSettingsToEmbeddingTask() {
        var model = createEmbeddingModel(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, null));
        var overridden = TencentCloudEmbeddingsModel.of(model, Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "STS"));
        assertThat(overridden.getMultimodalTaskSettings(), is(new TencentCloudMultimodalEmbeddingsTaskSettings(InputType.INGEST, "STS")));
        assertThat(overridden.uri(), is(model.uri()));
    }

    public void testOf_IgnoresRequestTaskSettingsForTextEmbeddingTask() {
        var model = createModel(new TencentCloudTextEmbeddingServiceSettings("bge-m3", null, null, null, null, null));
        assertSame(model, TencentCloudEmbeddingsModel.of(model, Map.of(TencentCloudMultimodalEmbeddingsTaskSettings.PROMPT_NAME, "STS")));
    }

    public static TencentCloudEmbeddingsModel createEmbeddingModel(TencentCloudMultimodalEmbeddingsTaskSettings taskSettings) {
        return new TencentCloudEmbeddingsModel(
            "test-inference-id",
            TaskType.EMBEDDING,
            new TencentCloudEmbeddingServiceSettings("WeMM-Embedding-2B", null, null, null, null, null),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }

    public static TencentCloudEmbeddingsModel createModel(TencentCloudTextEmbeddingServiceSettings serviceSettings) {
        return new TencentCloudEmbeddingsModel(
            "test-inference-id",
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString("sk-test".toCharArray()))
        );
    }
}
