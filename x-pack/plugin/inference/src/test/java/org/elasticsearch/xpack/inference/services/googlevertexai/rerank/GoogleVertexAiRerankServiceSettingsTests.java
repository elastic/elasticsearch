/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiRerankServiceSettings> {
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-test-project-id";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = new GoogleVertexAiRerankServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.RERANK);

        MatcherAssert.assertThat(
            serviceSettings,
            is(new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new GoogleVertexAiRerankServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.RERANK);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiRerankServiceSettings(
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var projectId = randomAlphaOfLength(10);
        var modelId = randomAlphaOfLengthOrNull(10);
        var rateLimitSettings = RateLimitSettingsTests.createRandom();

        var serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(new HashMap<>() {
            {
                put(GoogleVertexAiServiceFields.PROJECT_ID, projectId);
                put(ServiceFields.MODEL_ID, modelId);
                put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimitSettings.requestsPerTimeUnit())));
            }
        }, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new GoogleVertexAiRerankServiceSettings(projectId, modelId, rateLimitSettings)));
    }

    public void testFromMap_Request_OnlyMandatoryField() {
        var projectId = randomAlphaOfLength(10);

        var serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(new HashMap<>() {
            {
                put(GoogleVertexAiServiceFields.PROJECT_ID, projectId);
            }
        }, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new GoogleVertexAiRerankServiceSettings(projectId, null, new RateLimitSettings(300))));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "project_id": "%s",
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_PROJECT_ID, TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    public void testToXContent_NoModelId_DefaultRateLimit() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "project_id": "%s",
                "rate_limit": {
                    "requests_per_minute": 300
                }
            }
            """, TEST_PROJECT_ID)));
    }

    public void testFilteredXContentObject_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
                {
                    "project_id": "%s",
                    "model_id": "%s",
                    "rate_limit": {
                        "requests_per_minute": 300
                    }
                }
            """, TEST_PROJECT_ID, TEST_MODEL_ID)));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiRerankServiceSettings> instanceReader() {
        return GoogleVertexAiRerankServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings mutateInstance(GoogleVertexAiRerankServiceSettings instance) throws IOException {
        var projectId = instance.projectId();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(2)) {
            case 0 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(10));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new GoogleVertexAiRerankServiceSettings(projectId, modelId, rateLimitSettings);
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings mutateInstanceForVersion(
        GoogleVertexAiRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static GoogleVertexAiRerankServiceSettings createRandom() {
        return new GoogleVertexAiRerankServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }
}
