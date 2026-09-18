/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.REGION;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.ENDPOINT_ID_VALUE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OciGenAiEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiEmbeddingsServiceSettings> {

    private static final String MODEL_ID_VALUE = "cohere.embed-v4.0";

    public static OciGenAiEmbeddingsServiceSettings createRandom() {
        var useUrl = randomBoolean();
        var common = new OciGenAiServiceSettings.CommonSettings(
            useUrl && randomBoolean() ? null : randomAlphaOfLength(8),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            useUrl ? URI.create("https://" + randomAlphaOfLength(8) + ".example.com") : null,
            RateLimitSettingsTests.createRandom()
        );
        return new OciGenAiEmbeddingsServiceSettings(
            common,
            randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomIntBetween(1, 512),
            randomBoolean() ? null : randomFrom(SimilarityMeasure.values())
        );
    }

    public void testFromMap_Request_ParsesAllFields() {
        var map = OciGenAiTestUtils.serviceSettingsMap(REGION_VALUE, COMPARTMENT_ID, MODEL_ID_VALUE, ENDPOINT_ID_VALUE, null);
        map.put(DIMENSIONS, 512);
        map.put(MAX_INPUT_TOKENS, 128);
        map.put(SIMILARITY, "cosine");
        RateLimitSettingsTests.addRateLimitSettingsToMap(map, 42);

        var settings = OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.region(), is(REGION_VALUE));
        assertThat(settings.compartmentId(), is(COMPARTMENT_ID));
        assertThat(settings.modelId(), is(MODEL_ID_VALUE));
        assertThat(settings.endpointId(), is(ENDPOINT_ID_VALUE));
        assertTrue(settings.isDedicated());
        assertThat(settings.uri(), nullValue());
        assertThat(settings.dimensions(), is(512));
        assertTrue(settings.dimensionsSetByUser());
        assertThat(settings.maxInputTokens(), is(128));
        assertThat(settings.similarity(), is(SimilarityMeasure.COSINE));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(42)));
        assertThat(settings.apiFormat(), is(OciGenAiChatApiFormat.COHERE));
        assertTrue(map.isEmpty());
    }

    public void testFromMap_Request_MinimalSettings_UsesDefaults() {
        var settings = OciGenAiEmbeddingsServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE),
            ConfigurationParseContext.REQUEST
        );

        assertThat(settings.dimensions(), nullValue());
        assertFalse(settings.dimensionsSetByUser());
        assertThat(settings.maxInputTokens(), nullValue());
        assertThat(settings.similarity(), nullValue());
        assertThat(settings.endpointId(), nullValue());
        assertFalse(settings.isDedicated());
        assertThat(settings.rateLimitSettings(), is(OciGenAiServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
    }

    public void testFromMap_Request_AcceptsUrlWithoutRegion() {
        var settings = OciGenAiEmbeddingsServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap(null, COMPARTMENT_ID, MODEL_ID_VALUE, null, "https://private.example.com"),
            ConfigurationParseContext.REQUEST
        );

        assertThat(settings.region(), nullValue());
        assertThat(settings.uri(), is(URI.create("https://private.example.com")));
    }

    public void testFromMap_Request_ThrowsWhenNeitherRegionNorUrlProvided() {
        var map = OciGenAiTestUtils.serviceSettingsMap(null, COMPARTMENT_ID, MODEL_ID_VALUE, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] must contain either the [region] or the [url] setting"));
    }

    public void testFromMap_Request_ThrowsWhenRegionIsInvalid() {
        var map = OciGenAiTestUtils.serviceSettingsMap("US Chicago", COMPARTMENT_ID, MODEL_ID_VALUE, null, null);

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("Invalid value [US Chicago] for [region]"));
    }

    public void testFromMap_Request_ThrowsWhenCompartmentIdIsMissing() {
        var map = OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE);
        map.remove(
            OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE).keySet().stream().filter("compartment_id"::equals).findFirst().get()
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [compartment_id]"));
    }

    public void testFromMap_Request_ThrowsWhenDimensionsSetByUserIsProvided() {
        var map = OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE);
        map.put(ServiceFields.DIMENSIONS_SET_BY_USER, true);

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] does not allow the setting [dimensions_set_by_user]"));
    }

    public void testFromMap_Persistent_ThrowsWhenDimensionsSetByUserIsMissing() {
        var map = OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE);

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] does not contain the required setting [dimensions_set_by_user]")
        );
    }

    public void testFromMap_Persistent_ParsesDimensionsSetByUser() {
        var map = OciGenAiTestUtils.serviceSettingsMap(MODEL_ID_VALUE);
        map.put(DIMENSIONS, 1536);
        map.put(ServiceFields.DIMENSIONS_SET_BY_USER, false);

        var settings = OciGenAiEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(settings.dimensions(), is(1536));
        assertFalse(settings.dimensionsSetByUser());
    }

    public void testToXContent_WritesAllFieldsIncludingDimensionsSetByUser() throws IOException {
        var settings = createSettings(REGION_VALUE, null, 1024, true, 256, SimilarityMeasure.DOT_PRODUCT, new RateLimitSettings(3));

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "region": "us-chicago-1",
                "compartment_id": "%s",
                "model_id": "cohere.embed-v4.0",
                "rate_limit": { "requests_per_minute": 3 },
                "dimensions": 1024,
                "max_input_tokens": 256,
                "similarity": "dot_product",
                "dimensions_set_by_user": true
            }
            """, COMPARTMENT_ID))));
    }

    public void testFilteredXContent_DoesNotWriteDimensionsSetByUser_AndWritesUrlAndEndpoint() throws IOException {
        var settings = createSettings(null, URI.create("https://private.example.com"), null, false, null, null, new RateLimitSettings(3));

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.getFilteredXContentObject().toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "compartment_id": "%s",
                "model_id": "cohere.embed-v4.0",
                "endpoint_id": "%s",
                "url": "https://private.example.com",
                "rate_limit": { "requests_per_minute": 3 }
            }
            """, COMPARTMENT_ID, ENDPOINT_ID_VALUE))));
    }

    public void testUpdateServiceSettings_OnlyUpdatesMaxInputTokensAndRateLimit() {
        var settings = createSettings(REGION_VALUE, null, 1024, true, 256, SimilarityMeasure.COSINE, new RateLimitSettings(3));

        var updated = settings.updateServiceSettings(
            RateLimitSettingsTests.addRateLimitSettingsToMap(new HashMap<>(Map.of(MAX_INPUT_TOKENS, 100)), 50)
        );

        assertThat(updated.maxInputTokens(), is(100));
        assertThat(updated.rateLimitSettings(), is(new RateLimitSettings(50)));
        assertThat(updated.dimensions(), is(1024));
        assertThat(updated.similarity(), is(SimilarityMeasure.COSINE));
        assertThat(updated.common().withRateLimitSettings(new RateLimitSettings(3)), is(settings.common()));
    }

    private static OciGenAiEmbeddingsServiceSettings createSettings(
        @Nullable String region,
        @Nullable URI uri,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {
        return new OciGenAiEmbeddingsServiceSettings(
            OciGenAiTestUtils.commonSettings(
                region,
                COMPARTMENT_ID,
                MODEL_ID_VALUE,
                uri == null ? null : ENDPOINT_ID_VALUE,
                uri,
                rateLimitSettings
            ),
            dimensionsSetByUser,
            dimensions,
            maxInputTokens,
            similarity
        );
    }

    public static Map<String, Object> getServiceSettingsMap(
        String modelId,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity
    ) {
        var map = OciGenAiTestUtils.serviceSettingsMap(modelId);
        if (dimensions != null) {
            map.put(DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            map.put(SIMILARITY, similarity.toString());
        }
        map.put(REGION, REGION_VALUE);
        return map;
    }

    @Override
    protected Writeable.Reader<OciGenAiEmbeddingsServiceSettings> instanceReader() {
        return OciGenAiEmbeddingsServiceSettings::new;
    }

    @Override
    protected OciGenAiEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiEmbeddingsServiceSettings mutateInstance(OciGenAiEmbeddingsServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiEmbeddingsServiceSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiEmbeddingsServiceSettings mutateInstanceForVersion(
        OciGenAiEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
