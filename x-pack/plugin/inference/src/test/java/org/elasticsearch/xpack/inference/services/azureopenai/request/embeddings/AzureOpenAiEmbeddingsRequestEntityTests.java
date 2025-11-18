/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiEmbeddingsRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AzureOpenAiEmbeddingsRequestEntityTests extends ESTestCase {

    public void testXContent_WritesUserWhenDefined() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), null, "testuser", null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"user":"testuser"}"""));
    }

    public void testXContent_WritesInputTypeWhenDefined() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), InputType.CLASSIFICATION, "testuser", null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"user":"testuser","input_type":"classification"}"""));
    }

    public void testXContent_DoesNotWriteUserWhenItIsNull() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), null, null, null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"]}"""));
    }

    public void testXContent_DoesNotWriteDimensionsWhenNotSetByUser() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), null, null, 100, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"]}"""));
    }

    public void testXContent_DoesNotWriteDimensionsWhenNull_EvenIfSetByUserIsTrue() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), null, null, null, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"]}"""));
    }

    public void testXContent_WritesDimensionsWhenNonNull_AndSetByUserIsTrue() throws IOException {
        var entity = new AzureOpenAiEmbeddingsRequestEntity(List.of("abc"), null, null, 100, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"dimensions":100}"""));
    }
}
