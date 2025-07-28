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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiRerankServiceSettings> {

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var projectId = randomAlphaOfLength(10);
        var modelId = randomFrom(new String[] { null, randomAlphaOfLength(10) });

        var serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(new HashMap<>() {
            {
                put(GoogleVertexAiServiceFields.PROJECT_ID, projectId);
                put(ServiceFields.MODEL_ID, modelId);
            }
        }, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new GoogleVertexAiRerankServiceSettings(projectId, modelId, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings("projectId", "modelId", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "project_id": "projectId",
                "model_id": "modelId",
                "rate_limit": {
                    "requests_per_minute": 300
                }
            }
            """));
    }

    public void testToXContent_DoesNotWriteModelIfNull() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings("projectId", null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "project_id": "projectId",
                "rate_limit": {
                    "requests_per_minute": 300
                }
            }
            """));
    }

    public void testFilteredXContentObject_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings("projectId", "modelId", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
                {
                    "project_id": "projectId",
                    "model_id": "modelId",
                    "rate_limit": {
                        "requests_per_minute": 300
                    }
                }
            """));
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
        return randomValueOtherThan(instance, GoogleVertexAiRerankServiceSettingsTests::createRandom);
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
            randomFrom(new String[] { null, randomAlphaOfLength(10) }),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }
}
