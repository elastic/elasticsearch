/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiService;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.INPUT_TYPE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiEmbeddingsModelTests extends ESTestCase {

    public void testUri_IsDerivedFromTheRegion() {
        var model = createModel(null, "cohere.embed-v4.0", null, null, null, null);

        assertThat(
            model.uri(),
            is(URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/embedText"))
        );
    }

    public void testUri_IsDerivedFromTheConfiguredUrl() {
        var serviceSettings = new OciGenAiEmbeddingsServiceSettings(
            OciGenAiTestUtils.commonSettings(
                null,
                COMPARTMENT_ID,
                "cohere.embed-v4.0",
                null,
                URI.create("https://private.example.com/base/"),
                null
            ),
            false,
            null,
            null,
            null
        );
        var model = new OciGenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            OciGenAiService.NAME,
            serviceSettings,
            OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            OciGenAiTestUtils.createSecretSettings()
        );

        assertThat(model.uri(), is(URI.create("https://private.example.com/base/20231130/actions/embedText")));
    }

    public void testUri_UsesTheOverride() {
        var model = createModel("http://127.0.0.1:1234", "cohere.embed-v4.0", null, null, null, null);

        assertThat(model.uri(), is(URI.create("http://127.0.0.1:1234")));
    }

    public void testOf_OverridesTaskSettingsFromRequest() {
        var model = createModel(null, "cohere.embed-v4.0", null, null, InputType.INGEST, Truncation.END);

        var overridden = OciGenAiEmbeddingsModel.of(model, new HashMap<>(Map.of(INPUT_TYPE, "search")));

        assertThat(overridden.getTaskSettings().getInputType(), is(InputType.SEARCH));
        assertThat(overridden.getTaskSettings().getTruncation(), is(Truncation.END));
        assertThat(overridden.uri(), is(model.uri()));
        assertThat(overridden.requestSigner(), sameInstance(model.requestSigner()));
    }

    public void testRateLimitGroupingHash_IgnoresTheSigningKey() {
        var model = createModel(null, "cohere.embed-v4.0", null, null, null, null);
        var otherKey = new OciGenAiEmbeddingsModel(
            "other-id",
            TaskType.TEXT_EMBEDDING,
            OciGenAiService.NAME,
            model.getServiceSettings(),
            model.getTaskSettings(),
            null,
            OciGenAiTestUtils.createSecretSettings(OciGenAiTestUtils.toPkcs8Pem(OciGenAiTestUtils.generateKeyPair().getPrivate()))
        );

        assertThat(otherKey.rateLimitGroupingHash(), is(model.rateLimitGroupingHash()));
    }

    public static OciGenAiEmbeddingsModel createModel(
        @Nullable String url,
        String modelId,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable InputType inputType,
        @Nullable Truncation truncation
    ) {
        return createModel(url, modelId, dimensions, dimensions != null, similarity, inputType, truncation, null, null);
    }

    public static OciGenAiEmbeddingsModel createModel(
        @Nullable String url,
        String modelId,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable SimilarityMeasure similarity,
        @Nullable InputType inputType,
        @Nullable Truncation truncation,
        @Nullable String endpointId,
        @Nullable Integer maxInputTokens
    ) {
        var serviceSettings = new OciGenAiEmbeddingsServiceSettings(
            OciGenAiTestUtils.commonSettings(REGION_VALUE, COMPARTMENT_ID, modelId, endpointId, null, new RateLimitSettings(1000)),
            dimensionsSetByUser,
            dimensions,
            maxInputTokens,
            similarity
        );
        return new OciGenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            OciGenAiService.NAME,
            url,
            serviceSettings,
            new OciGenAiEmbeddingsTaskSettings(inputType, truncation),
            OciGenAiTestUtils.createSecretSettings(),
            OciGenAiTestUtils.fixedAuthHeader()
        );
    }
}
