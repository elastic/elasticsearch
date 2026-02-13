/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class OpenAiEmbeddingsRequestEntityTests extends ESTestCase {

    public void testXContent_WritesUserWhenDefined() throws IOException {
        var entity = new OpenAiEmbeddingsRequestEntity(List.of("abc"), "model", "user", null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","user":"user"}"""));
    }

    public void testXContent_DoesNotWriteUserWhenItIsNull() throws IOException {
        var entity = new OpenAiEmbeddingsRequestEntity(List.of("abc"), "model", null, null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model"}"""));
    }

    public void testXContent_DoesNotWriteDimensionsWhenNotSetByUser() throws IOException {
        var entity = new OpenAiEmbeddingsRequestEntity(List.of("abc"), "model", null, 100, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model"}"""));
    }

    public void testXContent_DoesNotWriteDimensionsWhenNull_EvenIfSetByUserIsTrue() throws IOException {
        var entity = new OpenAiEmbeddingsRequestEntity(List.of("abc"), "model", null, null, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model"}"""));
    }

    public void testXContent_WritesDimensionsWhenNonNull_AndSetByUserIsTrue() throws IOException {
        var entity = new OpenAiEmbeddingsRequestEntity(List.of("abc"), "model", null, 100, true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"input":["abc"],"model":"model","dimensions":100}"""));
    }
}
