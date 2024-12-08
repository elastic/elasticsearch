/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AmazonBedrockEmbeddingsServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var maxInputTokens = 512;
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(
            createEmbeddingsRequestSettingsMap(region, model, provider, null, null, maxInputTokens, SimilarityMeasure.COSINE),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    region,
                    model,
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var maxInputTokens = 512;
        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, null, maxInputTokens, SimilarityMeasure.COSINE);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    region,
                    model,
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    new RateLimitSettings(3)
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_IsFalse_WhenDimensionsAreNotPresent() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var maxInputTokens = 512;
        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, null, maxInputTokens, SimilarityMeasure.COSINE);
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    region,
                    model,
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_Request_DimensionsSetByUser_ShouldThrowWhenPresent() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var maxInputTokens = 512;

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, true, maxInputTokens, SimilarityMeasure.COSINE);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] does not allow the setting [%s];", DIMENSIONS_SET_BY_USER)
            )
        );
    }

    public void testFromMap_Request_Dimensions_ShouldThrowWhenPresent() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var dims = 128;

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, dims, null, null, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[service_settings] does not allow the setting [%s]", DIMENSIONS))
        );
    }

    public void testFromMap_Request_MaxTokensShouldBePositiveInteger() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var maxInputTokens = -128;

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, null, maxInputTokens, null);

        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[%s] must be a positive integer", MAX_INPUT_TOKENS))
        );
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";
        var dims = 1536;
        var maxInputTokens = 512;

        var settingsMap = createEmbeddingsRequestSettingsMap(
            region,
            model,
            provider,
            dims,
            false,
            maxInputTokens,
            SimilarityMeasure.COSINE
        );
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    region,
                    model,
                    AmazonBedrockProvider.AMAZONTITAN,
                    dims,
                    false,
                    maxInputTokens,
                    SimilarityMeasure.COSINE,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenDimensionsIsNull() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, true, null, null);
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new AmazonBedrockEmbeddingsServiceSettings(region, model, AmazonBedrockProvider.AMAZONTITAN, null, true, null, null, null))
        );
    }

    public void testFromMap_PersistentContext_DoesNotThrowException_WhenSimilarityIsPresent() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, null, true, null, SimilarityMeasure.DOT_PRODUCT);
        var serviceSettings = AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new AmazonBedrockEmbeddingsServiceSettings(
                    region,
                    model,
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    true,
                    null,
                    SimilarityMeasure.DOT_PRODUCT,
                    null
                )
            )
        );
    }

    public void testFromMap_PersistentContext_ThrowsException_WhenDimensionsSetByUserIsNull() {
        var region = "region";
        var model = "model-id";
        var provider = "amazontitan";

        var settingsMap = createEmbeddingsRequestSettingsMap(region, model, provider, 1, null, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockEmbeddingsServiceSettings.fromMap(settingsMap, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];")
        );
    }

    public void testToXContent_WritesDimensionsSetByUserTrue() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            "testregion",
            "testmodel",
            AmazonBedrockProvider.AMAZONTITAN,
            null,
            true,
            null,
            null,
            new RateLimitSettings(2)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"region":"testregion","model":"testmodel","provider":"AMAZONTITAN",""" + """
            "rate_limit":{"requests_per_minute":2},"dimensions_set_by_user":true}"""));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            "testregion",
            "testmodel",
            AmazonBedrockProvider.AMAZONTITAN,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"region":"testregion","model":"testmodel","provider":"AMAZONTITAN",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512,"dimensions_set_by_user":false}"""));
    }

    public void testToFilteredXContent_WritesAllValues_ExceptDimensionsSetByUser() throws IOException {
        var entity = new AmazonBedrockEmbeddingsServiceSettings(
            "testregion",
            "testmodel",
            AmazonBedrockProvider.AMAZONTITAN,
            1024,
            false,
            512,
            null,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"region":"testregion","model":"testmodel","provider":"AMAZONTITAN",""" + """
            "rate_limit":{"requests_per_minute":3},"dimensions":1024,"max_input_tokens":512}"""));
    }

    public static HashMap<String, Object> createEmbeddingsRequestSettingsMap(
        String region,
        String model,
        String provider,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        var map = new HashMap<String, Object>(Map.of(REGION_FIELD, region, MODEL_FIELD, model, PROVIDER_FIELD, provider));

        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }

        if (dimensionsSetByUser != null) {
            map.put(DIMENSIONS_SET_BY_USER, dimensionsSetByUser.equals(Boolean.TRUE));
        }

        if (maxTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxTokens);
        }

        if (similarityMeasure != null) {
            map.put(SIMILARITY, similarityMeasure.toString());
        }

        return map;
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings mutateInstanceForVersion(
        AmazonBedrockEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockEmbeddingsServiceSettings> instanceReader() {
        return AmazonBedrockEmbeddingsServiceSettings::new;
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockEmbeddingsServiceSettings mutateInstance(AmazonBedrockEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, AmazonBedrockEmbeddingsServiceSettingsTests::createRandom);
    }

    private static AmazonBedrockEmbeddingsServiceSettings createRandom() {
        return new AmazonBedrockEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomFrom(AmazonBedrockProvider.values()),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomBoolean(),
            randomFrom(new Integer[] { null, randomNonNegativeInt() }),
            randomFrom(new SimilarityMeasure[] { null, randomFrom(SimilarityMeasure.values()) }),
            RateLimitSettingsTests.createRandom()
        );
    }
}
