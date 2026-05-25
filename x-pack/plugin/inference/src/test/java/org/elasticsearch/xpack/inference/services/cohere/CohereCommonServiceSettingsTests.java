/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CohereCommonServiceSettingsTests extends AbstractWireSerializingTestCase<CohereCommonServiceSettings> {

    public static CohereCommonServiceSettings createRandom() {
        return new CohereCommonServiceSettings(
            randomUriOrNull(),
            randomAlphaOfLengthOrNull(15),
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereCommonServiceSettings.CohereApiVersion.values())
        );
    }

    private static URI randomUriOrNull() {
        return randomBoolean() ? null : ServiceUtils.createUri(randomAlphaOfLength(10));
    }

    public void testFromMap_Request_SetModelId() {
        var serviceSettings = CohereCommonServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "my-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.modelId(), is("my-model"));
        assertThat(serviceSettings.apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V2));
    }

    public void testFromMap_Request_V2_RequiresModelId() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> CohereCommonServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(CohereCommonServiceSettings.MODEL_REQUIRED_FOR_V2_API));
    }

    public void testFromMap_Request_DeprecatedModelField() {
        var serviceSettings = CohereCommonServiceSettings.fromMap(
            new HashMap<>(Map.of(CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, "old-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.modelId(), is("old-model"));
    }

    public void testFromMap_Persistent_EmptyMap_DefaultsToV1() {
        var serviceSettings = CohereCommonServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V1));
        assertThat(serviceSettings.modelId(), is((String) null));
    }

    public void testFromMap_Persistent_WithApiVersion() {
        var serviceSettings = CohereCommonServiceSettings.fromMap(
            new HashMap<>(Map.of(CohereCommonServiceSettings.API_VERSION, "v2", ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings.apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V2));
        assertThat(serviceSettings.modelId(), is("m"));
    }

    public void testToXContent_ExposedFields_DoesNotContainApiVersion() throws IOException {
        var serviceSettings = new CohereCommonServiceSettings(
            "test-model",
            new RateLimitSettings(20),
            CohereCommonServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
              "model_id": "test-model",
              "rate_limit": {
                "requests_per_minute": 20
              }
            }
            """)));
        assertThat(xContentResult.contains(CohereCommonServiceSettings.API_VERSION), is(false));
    }

    public void testToXContentFragment_ContainsApiVersion() throws IOException {
        var serviceSettings = new CohereCommonServiceSettings(
            "test-model",
            new RateLimitSettings(20),
            CohereCommonServiceSettings.CohereApiVersion.V2
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, containsString(CohereCommonServiceSettings.API_VERSION));
        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "model_id": "test-model",
              "rate_limit": {
                "requests_per_minute": 20
              },
              "api_version": "%s"
            }
            """, CohereCommonServiceSettings.CohereApiVersion.V2))));
    }

    @Override
    protected Writeable.Reader<CohereCommonServiceSettings> instanceReader() {
        return CohereCommonServiceSettings::new;
    }

    @Override
    protected CohereCommonServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereCommonServiceSettings mutateInstance(CohereCommonServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();

        switch (randomInt(3)) {
            case 0 -> uri = randomValueOtherThan(uri, CohereCommonServiceSettingsTests::randomUriOrNull);
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(15));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 3 -> apiVersion = randomValueOtherThan(
                apiVersion,
                () -> randomFrom(CohereCommonServiceSettings.CohereApiVersion.values())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
    }
}
