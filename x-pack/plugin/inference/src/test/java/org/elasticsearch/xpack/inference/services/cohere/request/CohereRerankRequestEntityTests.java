/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class CohereRerankRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new CohereRerankRequestEntity(
            "query",
            List.of("abc"),
            Boolean.TRUE,
            22,
            new CohereRerankTaskSettings(null, null, 3),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":true,"top_n":22,"max_chunks_per_doc":3}"""));
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new CohereRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new CohereRerankTaskSettings(null, null, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"model":"model","query":"query","documents":["abc"]}"""));
    }

    public void testXContent_PrefersRootLevelReturnDocumentsAndTopN() throws IOException {
        var entity = new CohereRerankRequestEntity(
            "query",
            List.of("abc"),
            Boolean.FALSE,
            99,
            new CohereRerankTaskSettings(33, Boolean.TRUE, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":false,"top_n":99}"""));
    }

    public void testXContent_UsesTaskSettingsIfNoRootOptionsDefined() throws IOException {
        var entity = new CohereRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new CohereRerankTaskSettings(33, Boolean.TRUE, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"model":"model","query":"query","documents":["abc"],"return_documents":true,"top_n":33}"""));
    }
}
