/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereRerankServiceSettingsTests extends AbstractWireSerializingTestCase<CohereRerankServiceSettings> {
    public static CohereRerankServiceSettings createRandom() {
        var commonSettings = CohereServiceSettingsTests.createRandom();

        return new CohereRerankServiceSettings(commonSettings);
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereRerankServiceSettings(
            new CohereServiceSettings("url", SimilarityMeasure.COSINE, 5, 10, "model_id", new RateLimitSettings(3))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        // TODO we probably shouldn't allow configuring these fields for reranking
        assertThat(xContentResult, is("""
            {"url":"url","similarity":"cosine","dimensions":5,"max_input_tokens":10,"model_id":"model_id",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public void testToXContent_WritesAllValues_Except_RateLimit() throws IOException {
        var serviceSettings = new CohereRerankServiceSettings(
            new CohereServiceSettings("url", SimilarityMeasure.COSINE, 5, 10, "model_id", new RateLimitSettings(3))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = serviceSettings.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        // TODO we probably shouldn't allow configuring these fields for reranking
        assertThat(xContentResult, is("""
            {"url":"url","similarity":"cosine","dimensions":5,"max_input_tokens":10,"model_id":"model_id"}"""));
    }

    @Override
    protected Writeable.Reader<CohereRerankServiceSettings> instanceReader() {
        return CohereRerankServiceSettings::new;
    }

    @Override
    protected CohereRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereRerankServiceSettings mutateInstance(CohereRerankServiceSettings instance) throws IOException {
        return null;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}
