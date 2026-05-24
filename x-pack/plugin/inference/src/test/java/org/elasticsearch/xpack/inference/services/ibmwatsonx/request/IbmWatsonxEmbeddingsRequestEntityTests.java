/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class IbmWatsonxEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_SingleRequest() throws IOException {
        var entity = new IbmWatsonxEmbeddingsRequestEntity(List.of("abc"), "model", "project_id");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
                    {
                        "inputs": ["abc"],
                        "model_id":"model",
                        "project_id": "project_id"
                    }
            """));
    }

    public void testXContent_MultipleRequests() throws IOException {
        var entity = new IbmWatsonxEmbeddingsRequestEntity(List.of("abc", "def"), "model", "project_id");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
                    {
                        "inputs": ["abc", "def"],
                        "model_id":"model",
                        "project_id": "project_id"
                    }
            """));
    }
}
