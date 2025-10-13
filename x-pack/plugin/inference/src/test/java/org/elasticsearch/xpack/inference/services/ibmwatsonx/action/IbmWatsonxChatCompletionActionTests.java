/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ChatCompletionActionTests;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxChatCompletionRequest;

import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionCreator.COMPLETION_HANDLER;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionCreator.USER_ROLE;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModelTests.createModel;

public class IbmWatsonxChatCompletionActionTests extends ChatCompletionActionTests {
    public static final URI TEST_URI = URI.create("abc.com");

    protected ExecutableAction createAction(String url, Sender sender) throws URISyntaxException {
        var model = createModel(TEST_URI, randomAlphaOfLength(8), randomAlphaOfLength(8), randomAlphaOfLength(8), randomAlphaOfLength(8));
        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            COMPLETION_HANDLER,
            inputs -> new IbmWatsonxChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );
        var errorMessage = constructFailedToSendRequestMessage("watsonx chat completions");
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, "watsonx chat completions");
    }

    protected String getFailedToSendError() {
        return "Failed to send watsonx chat completions request. Cause: failed";
    }

    protected String getOneInputError() {
        return "watsonx chat completions only accepts 1 input";
    }
}
