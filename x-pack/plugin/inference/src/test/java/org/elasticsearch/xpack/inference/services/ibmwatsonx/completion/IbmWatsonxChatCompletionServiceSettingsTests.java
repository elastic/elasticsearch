/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<IbmWatsonxChatCompletionServiceSettings> {

    private static IbmWatsonxChatCompletionServiceSettings createRandom() {
        URI uri = null;
        try {
            uri = new URI("abc.com");
        } catch (Exception ignored) {}

        return new IbmWatsonxChatCompletionServiceSettings(
            uri,
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = randomAlphaOfLength(8);
        var projectId = randomAlphaOfLength(8);
        URI uri = null;
        try {
            uri = new URI("abc.com");
        } catch (Exception ignored) {}
        var apiVersion = randomAlphaOfLength(8);

        var serviceSettings = IbmWatsonxChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    uri.toString(),
                    IbmWatsonxServiceFields.API_VERSION,
                    apiVersion,
                    ServiceFields.MODEL_ID,
                    model,
                    IbmWatsonxServiceFields.PROJECT_ID,
                    projectId
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, model, projectId, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        URI uri = null;
        try {
            uri = new URI("abc.com");
        } catch (Exception ignored) {}
        var entity = new IbmWatsonxChatCompletionServiceSettings(uri, "2024-05-02", "model", "project_id", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "url":"abc.com",
                "api_version":"2024-05-02",
                "model_id":"model",
                "project_id":"project_id",
                "rate_limit": {
                    "requests_per_minute":120
                }
            }"""));
    }

    @Override
    protected Writeable.Reader<IbmWatsonxChatCompletionServiceSettings> instanceReader() {
        return IbmWatsonxChatCompletionServiceSettings::new;
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings mutateInstance(IbmWatsonxChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, IbmWatsonxChatCompletionServiceSettingsTests::createRandom);
    }
}
