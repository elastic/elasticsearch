/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class FireworksAiEmbeddingsRequestEntityTests extends ESTestCase {

    public void testXContent_WritesInputAndModel() throws IOException {
        var entity = new FireworksAiEmbeddingsRequestEntity(List.of("abc"), "model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input": ["abc"],
                "model": "model"
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testXContent_WritesMultipleInputs() throws IOException {
        var entity = new FireworksAiEmbeddingsRequestEntity(List.of("abc", "def"), "model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input": ["abc", "def"],
                "model": "model"
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testXContent_WritesDimensionsWhenNonNull() throws IOException {
        var entity = new FireworksAiEmbeddingsRequestEntity(List.of("abc"), "model", 100);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input": ["abc"],
                "model": "model",
                "dimensions": 100
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testXContent_DoesNotWriteDimensionsWhenNull() throws IOException {
        var entity = new FireworksAiEmbeddingsRequestEntity(List.of("abc"), "model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "input": ["abc"],
                "model": "model"
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    public void testThrows_WhenInputIsNull() {
        expectThrows(NullPointerException.class, () -> new FireworksAiEmbeddingsRequestEntity(null, "model", null));
    }

    public void testThrows_WhenModelIsNull() {
        expectThrows(NullPointerException.class, () -> new FireworksAiEmbeddingsRequestEntity(List.of("abc"), null, null));
    }
}
