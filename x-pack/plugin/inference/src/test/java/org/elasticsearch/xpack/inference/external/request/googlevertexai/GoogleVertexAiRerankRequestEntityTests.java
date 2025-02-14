/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googlevertexai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.MatcherAssert.assertThat;

public class GoogleVertexAiRerankRequestEntityTests extends ESTestCase {
    public void testXContent_SingleRequest_WritesModelAndTopNIfDefined() throws IOException {
        var entity = new GoogleVertexAiRerankRequestEntity("query", List.of("abc"), "model", 8);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "records": [
                    {
                        "id": "0",
                        "content": "abc"
                    }
                ],
                "topN": 8
            }
            """));
    }

    public void testXContent_SingleRequest_DoesNotWriteModelAndTopNIfNull() throws IOException {
        var entity = new GoogleVertexAiRerankRequestEntity("query", List.of("abc"), null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "records": [
                    {
                        "id": "0",
                        "content": "abc"
                    }
                ]
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesModelAndTopNIfDefined() throws IOException {
        var entity = new GoogleVertexAiRerankRequestEntity("query", List.of("abc", "def"), "model", 8);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "records": [
                    {
                        "id": "0",
                        "content": "abc"
                    },
                    {
                        "id": "1",
                        "content": "def"
                    }
                ],
                "topN": 8
            }
            """));
    }

    public void testXContent_MultipleRequests_DoesNotWriteModelAndTopNIfNull() throws IOException {
        var entity = new GoogleVertexAiRerankRequestEntity("query", List.of("abc", "def"), null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "records": [
                    {
                        "id": "0",
                        "content": "abc"
                    },
                    {
                        "id": "1",
                        "content": "def"
                    }
                ]
            }
            """));
    }

}
