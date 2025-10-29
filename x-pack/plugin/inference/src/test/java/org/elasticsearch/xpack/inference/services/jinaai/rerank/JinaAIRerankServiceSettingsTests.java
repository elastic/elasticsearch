/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.LongDocumentStrategy;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RerankServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
import static org.hamcrest.Matchers.containsString;

public class JinaAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIRerankServiceSettings> {
    public static JinaAIRerankServiceSettings createRandom() {
        return new JinaAIRerankServiceSettings(
            RerankServiceSettingsTests.createRandom(),
            new JinaAIServiceSettings(
                randomFrom(new String[] { null, Strings.format("http://%s.com", randomAlphaOfLength(8)) }),
                randomAlphaOfLength(10),
                RateLimitSettingsTests.createRandom()
            )
        );
    }

    public void testFromMap_RerankServiceSettingsInvalid_ThrowsValidationException() {
        var url = randomAlphaOfLength(10);
        var model = randomAlphaOfLength(10);

        Map<String, Object> map = getServiceSettingsMap(null, randomNonNegativeInt(), url, model);

        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> JinaAIRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );
        assertThat(
            exception.getMessage(),
            containsString("Setting [max_chunks_per_doc] cannot be set without setting [long_document_strategy]")
        );
    }

    public void testFromMap_JinaAiServiceSettingsInvalid_ThrowsValidationException() {
        var rerankServiceSettings = RerankServiceSettingsTests.createRandom();
        var longDocumentStrategy = rerankServiceSettings.getLongDocumentStrategy();
        var maxChunksPerDoc = rerankServiceSettings.getMaxChunksPerDoc();
        var url = "http://www.abc.com";

        Map<String, Object> map = getServiceSettingsMap(longDocumentStrategy, maxChunksPerDoc, url, null);

        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> JinaAIRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );
        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testFromMap_AllValuesProvided_CreatesJinaAIRerankServiceSettings() {
        var longDocumentStrategy = LongDocumentStrategy.CHUNK;
        var maxChunksPerDoc = randomNonNegativeInt();
        var url = "http://www.abc.com";
        var model = randomAlphaOfLength(10);

        Map<String, Object> map = getServiceSettingsMap(longDocumentStrategy, maxChunksPerDoc, url, model);

        var serviceSettings = JinaAIRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);
        assertExpectedServiceSettings(serviceSettings, longDocumentStrategy, maxChunksPerDoc, url, model);
    }

    private Map<String, Object> getServiceSettingsMap(
        LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc,
        String url,
        String model
    ) {
        Map<String, Object> map = new HashMap<>();
        if (longDocumentStrategy != null) {
            map.put(RerankServiceSettings.LONG_DOCUMENT_STRATEGY, longDocumentStrategy.strategyName);
        }

        if (maxChunksPerDoc != null) {
            map.put(RerankServiceSettings.MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        }

        map.putAll(JinaAIServiceSettingsTests.getServiceSettingsMap(url, model));

        return map;
    }

    private void assertExpectedServiceSettings(
        JinaAIRerankServiceSettings serviceSettings,
        LongDocumentStrategy longDocumentStrategy,
        Integer maxChunksPerDoc,
        String url,
        String model
    ) {
        assertEquals(longDocumentStrategy, serviceSettings.getLongDocumentStrategy());
        assertEquals(maxChunksPerDoc, serviceSettings.getMaxChunksPerDoc());
        JinaAIServiceSettings commonSettings = serviceSettings.getCommonSettings();
        assertEquals(url, commonSettings.uri().toString());
        assertEquals(model, commonSettings.modelId());
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var longDocumentStrategy = randomFrom(LongDocumentStrategy.values());
        var maxChunksPerDoc = randomNonNegativeInt();
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new JinaAIRerankServiceSettings(
            new RerankServiceSettings(longDocumentStrategy, maxChunksPerDoc),
            new JinaAIServiceSettings(url, model, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "long_document_strategy":"%s",
                "max_chunks_per_doc":%d,
                "url":"%s",
                "model_id":"%s",
                "rate_limit": {
                    "requests_per_minute": 2000
                }
            }
            """, longDocumentStrategy.strategyName, maxChunksPerDoc, url, model, DEFAULT_RATE_LIMIT_SETTINGS.requestsPerTimeUnit())));
    }

    @Override
    protected Writeable.Reader<JinaAIRerankServiceSettings> instanceReader() {
        return JinaAIRerankServiceSettings::new;
    }

    @Override
    protected JinaAIRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstance(JinaAIRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, JinaAIRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstanceForVersion(JinaAIRerankServiceSettings instance, TransportVersion version) {
        // TODO: Update this once transport version is added to rerank service settings
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(JinaAIServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}
