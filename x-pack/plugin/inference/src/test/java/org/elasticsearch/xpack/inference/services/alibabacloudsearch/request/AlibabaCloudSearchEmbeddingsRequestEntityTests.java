/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AlibabaCloudSearchEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new AlibabaCloudSearchEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.SEARCH,
            new AlibabaCloudSearchEmbeddingsTaskSettings(InputType.INGEST)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"input_type":"query"}"""));
    }

    public void testXContent_WritesNoOptionalFields_WhenTheyAreNotDefined() throws IOException {
        var entity = new AlibabaCloudSearchEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            AlibabaCloudSearchEmbeddingsTaskSettings.EMPTY_SETTINGS
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"]}"""));
    }

    public void testXContent_InputType_Internal() throws IOException {
        var entity = new AlibabaCloudSearchEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new AlibabaCloudSearchEmbeddingsTaskSettings(InputType.INTERNAL_INGEST)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        MatcherAssert.assertThat(xContentResult, is("""
            {"input":["abc"],"input_type":"document"}"""));
    }
}
