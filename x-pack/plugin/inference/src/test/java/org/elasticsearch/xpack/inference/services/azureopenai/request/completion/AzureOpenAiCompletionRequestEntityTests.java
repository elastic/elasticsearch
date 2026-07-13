/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiCompletionRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AzureOpenAiCompletionRequestEntityTests extends ESTestCase {

    public void testXContent_WritesSingleMessage_DoesNotWriteUserWhenItIsNull() throws IOException {
        var entity = new AzureOpenAiCompletionRequestEntity(List.of("input"), null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"input"}],"n":1}"""));
    }

    public void testXContent_WritesSingleMessage_WriteUserWhenItIsNull() throws IOException {
        var entity = new AzureOpenAiCompletionRequestEntity(List.of("input"), "user", false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"input"}],"n":1,"user":"user"}"""));
    }
}
