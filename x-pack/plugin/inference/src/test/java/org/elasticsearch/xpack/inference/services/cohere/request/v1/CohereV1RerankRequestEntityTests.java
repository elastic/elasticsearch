/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v1;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

public class CohereV1RerankRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new CohereV1RerankRequest(
            "query",
            List.of("abc"),
            Boolean.TRUE,
            22,
            createModel("model", new CohereRerankTaskSettings(null, null, 3))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":true,"top_n":22,"max_chunks_per_doc":3}"""));
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new CohereV1RerankRequest(
            "query",
            List.of("abc"),
            null,
            null,
            createModel("model", new CohereRerankTaskSettings(null, null, null))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model","query":"query","documents":["abc"]}"""));
    }

    public void testXContent_PrefersRootLevelReturnDocumentsAndTopN() throws IOException {
        var entity = new CohereV1RerankRequest(
            "query",
            List.of("abc"),
            Boolean.FALSE,
            99,
            createModel("model", new CohereRerankTaskSettings(33, Boolean.TRUE, null))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":false,"top_n":99}"""));
    }

    public void testXContent_UsesTaskSettingsIfNoRootOptionsDefined() throws IOException {
        var entity = new CohereV1RerankRequest(
            "query",
            List.of("abc"),
            null,
            null,
            createModel("model", new CohereRerankTaskSettings(33, Boolean.TRUE, null))
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, CoreMatchers.is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":true,"top_n":33}"""));
    }

    private CohereRerankModel createModel(String modelId, CohereRerankTaskSettings taskSettings) {
        return new CohereRerankModel("inference_id", new CohereRerankServiceSettings("uri", modelId, null), taskSettings, null);
    }
}
