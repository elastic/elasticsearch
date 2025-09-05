/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModelTests.createModel;

public class IbmWatsonxChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";

    public void testModelUserFieldsSerialization() throws IOException, URISyntaxException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("test content"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        IbmWatsonxChatCompletionModel model = createModel(new URI("abc.com"), "apiVersion", "modelId", "projectId", "apiKey");

        IbmWatsonxChatCompletionRequestEntity entity = new IbmWatsonxChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expectedJson = """
            {
                "project_id": "projectId",
                "messages": [
                    {
                        "content": "test content",
                        "role": "user"
                    }
                ],
                "model": "modelId",
                "n": 1,
                "stream": true
            }
            """;
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }
}
