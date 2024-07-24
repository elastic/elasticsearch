/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ContentBlock;
import com.amazonaws.services.bedrockruntime.model.Message;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest.USER_ROLE;

public final class AmazonBedrockConverseUtils {

    public static List<Message> getConverseMessageList(List<String> messages) {
        List<Message> messageList = new ArrayList<>();
        for (String message : messages) {
            var messageContent = new ContentBlock().withText(message);
            var returnMessage = (new Message()).withRole(USER_ROLE).withContent(messageContent);
            messageList.add(returnMessage);
        }
        return messageList;
    }
}
