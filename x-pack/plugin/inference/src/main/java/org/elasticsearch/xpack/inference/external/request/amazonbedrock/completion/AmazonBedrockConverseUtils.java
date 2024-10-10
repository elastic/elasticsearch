/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.Message;

import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest.USER_ROLE;

public final class AmazonBedrockConverseUtils {

    public static List<Message> getConverseMessageList(List<String> texts) {
        return texts.stream()
            .map(text -> ContentBlock.builder().text(text).build())
            .map(content -> Message.builder().role(USER_ROLE).content(content).build())
            .toList();
    }
}
