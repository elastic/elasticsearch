/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultContentBlock;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.amazonbedrock.translation.ChatCompletionRole;

import java.util.List;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.DEFAULT_USER_MESSAGE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.HI_TEXT;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.PLEASE_CONTINUE_TEXT;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.TEXT_CONTENT_TYPE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.convertChatCompletionMessagesToConverse;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FUNCTION_TYPE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockConverseUtilsTests extends ESTestCase {

    private static final String SYSTEM_TEXT = "system";
    private static final String SYSTEM_INSTRUCTION = "system instruction";
    private static final String USER_TEXT = "user";
    private static final String USER1_TEXT = "user1";
    private static final String USER2_TEXT = "user2";
    private static final String USER_MESSAGE = "user message";
    private static final String USER_RESPONSE = "user response";
    private static final String ASSISTANT_TEXT = "assistant";
    private static final String ASSISTANT1_TEXT = "assistant1";
    private static final String ASSISTANT2_TEXT = "assistant2";
    private static final String ASSISTANT_REGULAR_MESSAGE = "assistant regular message";
    private static final String FIRST_ASSISTANT_MESSAGE = "first assistant message";
    private static final String ASSISTANT_RESPONSE = "assistant response";
    private static final String VALID_TEXT = "valid text";
    private static final String TOOL_RESULT = "tool result";
    private static final String PREVIOUS_TOOL_RESULT = "previous tool result";
    private static final String RESULT = "result";
    private static final String TOOL_CALL_ID = "tool-call-123";
    private static final String FUNCTION_NAME = "test_function";

    public void testConvertChatCompletionMessages_SystemMessage() {
        var messages = List.of(
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentObjects(
                    List.of(new UnifiedCompletionRequest.ContentObject(SYSTEM_TEXT, TEXT_CONTENT_TYPE))
                ),
                ChatCompletionRole.SYSTEM.toString(),
                null,
                null
            )
        );

        var result = convertChatCompletionMessagesToConverse(messages, false);

        assertThat(result.messages(), is(List.of()));
        assertThat(result.systemContent(), is(List.of(SystemContentBlock.builder().text(SYSTEM_TEXT).build())));
    }

    public void testConvertChatCompletionMessages_UserAndAssistantMessages() {
        var messages = List.of(
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString(USER_TEXT),
                ChatCompletionRole.USER.toString(),
                null,
                null
            ),
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString(ASSISTANT_TEXT),
                ChatCompletionRole.ASSISTANT.toString(),
                null,
                null
            )
        );

        var result = convertChatCompletionMessagesToConverse(messages, false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER_TEXT).build()).build(),
                    Message.builder().role(ConversationRole.ASSISTANT).content(ContentBlock.builder().text(ASSISTANT_TEXT).build()).build()
                )
            )
        );
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_ToolMessageWithToolsEnabled() {
        var toolCallId = TOOL_CALL_ID;
        var messages = List.of(
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString(RESULT),
                ChatCompletionRole.TOOL.toString(),
                toolCallId,
                null
            )
        );

        var result = convertChatCompletionMessagesToConverse(messages, true);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder()
                        .role(ConversationRole.USER)
                        .content(
                            ContentBlock.builder()
                                .toolResult(
                                    ToolResultBlock.builder()
                                        .toolUseId(toolCallId)
                                        .content(List.of(ToolResultContentBlock.builder().text(RESULT).build()))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            )
        );
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_ToolMessageWithToolsDisabled() {
        var messages = List.of(
            new UnifiedCompletionRequest.Message(
                new UnifiedCompletionRequest.ContentString(RESULT),
                ChatCompletionRole.TOOL.toString(),
                TOOL_CALL_ID,
                null
            )
        );

        var result = convertChatCompletionMessagesToConverse(messages, false);

        // tools disabled: tool messages are ignored
        assertThat(result.messages(), is(List.of()));
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_ToolChoiceNoneWithPreviousToolMessagesAndTextContent() {
        var toolCallId = TOOL_CALL_ID;

        var toolMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(PREVIOUS_TOOL_RESULT),
            ChatCompletionRole.TOOL.toString(),
            toolCallId,
            null
        );

        var assistantMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(ASSISTANT_REGULAR_MESSAGE),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            List.of(
                new UnifiedCompletionRequest.ToolCall(
                    toolCallId,
                    new UnifiedCompletionRequest.ToolCall.FunctionField("{\"test\":\"value\"}", "test_function"),
                    FUNCTION_TYPE
                )
            )
        );

        var userMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER_MESSAGE),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(toolMessage, assistantMessage, userMessage), false);

        // First message is assistant (after tool is dropped) so a default user message is prepended
        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(HI_TEXT).build()).build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(ContentBlock.builder().text(ASSISTANT_REGULAR_MESSAGE).build())
                        .build(),
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER_MESSAGE).build()).build()
                )
            )
        );
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_UserAndAssistantMessagesWithEmptyContent() {
        var userMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentObjects(
                List.of(
                    new UnifiedCompletionRequest.ContentObject("", TEXT_CONTENT_TYPE),
                    new UnifiedCompletionRequest.ContentObject(VALID_TEXT, TEXT_CONTENT_TYPE),
                    new UnifiedCompletionRequest.ContentObject("", TEXT_CONTENT_TYPE)
                )
            ),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var assistantMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentObjects(
                List.of(
                    new UnifiedCompletionRequest.ContentObject("", TEXT_CONTENT_TYPE),
                    new UnifiedCompletionRequest.ContentObject(ASSISTANT_RESPONSE, TEXT_CONTENT_TYPE)
                )
            ),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(userMessage, assistantMessage), false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(VALID_TEXT).build()).build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(ContentBlock.builder().text(ASSISTANT_RESPONSE).build())
                        .build()
                )
            )
        );
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_SystemMessageWithEmptyContent() {
        var systemMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentObjects(
                List.of(
                    new UnifiedCompletionRequest.ContentObject("", TEXT_CONTENT_TYPE),
                    new UnifiedCompletionRequest.ContentObject(SYSTEM_INSTRUCTION, TEXT_CONTENT_TYPE),
                    new UnifiedCompletionRequest.ContentObject("", TEXT_CONTENT_TYPE)
                )
            ),
            ChatCompletionRole.SYSTEM.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(systemMessage), false);

        assertThat(result.messages(), is(List.of()));
        assertThat(result.systemContent(), is(List.of(SystemContentBlock.builder().text(SYSTEM_INSTRUCTION).build())));
    }

    public void testConvertChatCompletionMessages_MergeConsecutiveUserMessages() {
        var user1 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER1_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );
        var user2 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER2_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(user1, user2), false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder()
                        .role(ConversationRole.USER)
                        .content(List.of(ContentBlock.builder().text(USER1_TEXT).build(), ContentBlock.builder().text(USER2_TEXT).build()))
                        .build()
                )
            )
        );
    }

    public void testConvertChatCompletionMessages_MergeConsecutiveAssistantMessages() {
        var user = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(HI_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );
        var assistant1 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(ASSISTANT1_TEXT),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );
        var assistant2 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(ASSISTANT2_TEXT),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(user, assistant1, assistant2), false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(HI_TEXT).build()).build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(
                            List.of(
                                ContentBlock.builder().text(ASSISTANT1_TEXT).build(),
                                ContentBlock.builder().text(ASSISTANT2_TEXT).build()
                            )
                        )
                        .build()
                )
            )
        );
    }

    public void testConvertChatCompletionMessages_ToolFollowedByTextInsertsAssistantMessage() {
        var toolCallId = TOOL_CALL_ID;

        var toolMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(TOOL_RESULT),
            ChatCompletionRole.TOOL.toString(),
            toolCallId,
            null
        );

        var userMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(toolMessage, userMessage), true);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder()
                        .role(ConversationRole.USER)
                        .content(
                            ContentBlock.builder()
                                .toolResult(
                                    ToolResultBlock.builder()
                                        .toolUseId(toolCallId)
                                        .content(List.of(ToolResultContentBlock.builder().text(TOOL_RESULT).build()))
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(ContentBlock.builder().text(PLEASE_CONTINUE_TEXT).build())
                        .build(),
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER_TEXT).build()).build()
                )
            )
        );
    }

    public void testConvertChatCompletionMessages_TextFollowedByToolInsertsAssistantMessage() {
        var toolCallId = TOOL_CALL_ID;

        var userMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var toolMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(TOOL_RESULT),
            ChatCompletionRole.TOOL.toString(),
            toolCallId,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(userMessage, toolMessage), true);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER_TEXT).build()).build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(ContentBlock.builder().text(PLEASE_CONTINUE_TEXT).build())
                        .build(),
                    Message.builder()
                        .role(ConversationRole.USER)
                        .content(
                            ContentBlock.builder()
                                .toolResult(
                                    ToolResultBlock.builder()
                                        .toolUseId(toolCallId)
                                        .content(List.of(ToolResultContentBlock.builder().text(TOOL_RESULT).build()))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            )
        );
    }

    public void testConvertChatCompletionMessages_NonConsecutiveSameRoleMessagesDontMerge() {
        var user1 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER1_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );
        var assistant = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(ASSISTANT_TEXT),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );
        var user2 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER2_TEXT),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(user1, assistant, user2), false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER1_TEXT).build()).build(),
                    Message.builder().role(ConversationRole.ASSISTANT).content(ContentBlock.builder().text(ASSISTANT_TEXT).build()).build(),
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER2_TEXT).build()).build()
                )
            )
        );
    }

    public void testConvertChatCompletionMessages_PrependUserMessageWhenFirstMessageIsAssistant() {
        var assistant = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(FIRST_ASSISTANT_MESSAGE),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );
        var user = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(USER_RESPONSE),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(assistant, user), false);

        assertThat(
            result.messages(),
            is(
                List.of(
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(HI_TEXT).build()).build(),
                    Message.builder()
                        .role(ConversationRole.ASSISTANT)
                        .content(ContentBlock.builder().text(FIRST_ASSISTANT_MESSAGE).build())
                        .build(),
                    Message.builder().role(ConversationRole.USER).content(ContentBlock.builder().text(USER_RESPONSE).build()).build()
                )
            )
        );
        assertThat(result.systemContent(), is(List.of()));
    }

    public void testConvertChatCompletionMessages_EmptyMessages() {
        var result = convertChatCompletionMessagesToConverse(List.of(), false);

        assertThat(result.messages(), empty());
        assertThat(result.systemContent(), empty());
    }

    public void testConvertChatCompletionMessages_MixedRolesWithEmptyContent() {
        var userMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(""),
            ChatCompletionRole.USER.toString(),
            null,
            null
        );
        var assistantMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(""),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(userMessage, assistantMessage), false);

        assertThat(result.messages(), empty());
        assertThat(result.systemContent(), empty());
    }

    public void testConvertChatCompletionMessages_ConsecutiveSystemMessages() {
        var systemMessage1Text = "System message 1";
        var systemMessage2Text = "System message 2";

        var systemMessage1 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(systemMessage1Text),
            ChatCompletionRole.SYSTEM.toString(),
            null,
            null
        );
        var systemMessage2 = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(systemMessage2Text),
            ChatCompletionRole.SYSTEM.toString(),
            null,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(systemMessage1, systemMessage2), false);

        assertThat(result.messages(), empty());
        assertThat(result.systemContent().size(), is(2));
        assertThat(result.systemContent().get(0).text(), is(systemMessage1Text));
        assertThat(result.systemContent().get(1).text(), is(systemMessage2Text));
    }

    public void testConvertChatCompletionMessages_ToolMessageWithoutToolsEnabled() {
        var toolMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(TOOL_RESULT),
            ChatCompletionRole.TOOL.toString(),
            TOOL_CALL_ID,
            null
        );

        var result = convertChatCompletionMessagesToConverse(List.of(toolMessage), false);

        assertThat(result.messages(), empty());
        assertThat(result.systemContent(), empty());
    }

    public void testConvertChatCompletionMessages_AssistantMessageWithToolCalls() {
        var toolCall = new UnifiedCompletionRequest.ToolCall(
            TOOL_CALL_ID,
            new UnifiedCompletionRequest.ToolCall.FunctionField("{\"key\":\"value\"}", FUNCTION_NAME),
            "function"
        );
        var assistantMessage = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString(ASSISTANT_RESPONSE),
            ChatCompletionRole.ASSISTANT.toString(),
            null,
            List.of(toolCall)
        );

        var result = convertChatCompletionMessagesToConverse(List.of(assistantMessage), true);

        // The default user message should get added
        assertThat(result.messages().size(), is(2));
        var firstMessage = result.messages().get(0);
        assertThat(firstMessage, is(DEFAULT_USER_MESSAGE));

        var secondMessage = result.messages().get(1);
        assertThat(secondMessage.role(), is(ConversationRole.ASSISTANT));
        assertThat(secondMessage.content().size(), is(2));
        assertThat(secondMessage.content().get(0).text(), is(ASSISTANT_RESPONSE));
        assertThat(secondMessage.content().get(1).toolUse().name(), is(FUNCTION_NAME));
        assertThat(secondMessage.content().get(1).toolUse().toolUseId(), is(TOOL_CALL_ID));
    }
}
