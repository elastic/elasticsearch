/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.model.AnyToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.AutoToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.SpecificToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionRequest.paramToDocumentMap;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.AUTO_TOOL_CHOICE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.FUNCTION_TYPE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.NONE_TOOL_CHOICE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.translation.Constants.REQUIRED_TOOL_CHOICE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockChatCompletionRequestTests extends ESTestCase {

    public void testConvertTools_ReturnsEmptyListForNoTools() {
        var a = AmazonBedrockChatCompletionRequest.convertTools(null);
        assertTrue(a.isEmpty());
    }

    public void testConvertTools_ForValidTool() {
        var description = "description";
        var name = "name";
        var tool = new UnifiedCompletionRequest.Tool(
            FUNCTION_TYPE,
            new UnifiedCompletionRequest.Tool.FunctionField(description, name, Map.of("key", "value"), null)
        );

        var convertedTools = AmazonBedrockChatCompletionRequest.convertTools(List.of(tool));

        assertThat(
            convertedTools,
            is(
                List.of(
                    Tool.builder()
                        .toolSpec(
                            ToolSpecification.builder()
                                .name(name)
                                .description(description)
                                .inputSchema(ToolInputSchema.fromJson(Document.fromMap(paramToDocumentMap(tool))))
                                .build()
                        )
                        .build()
                )
            )
        );
    }

    public void testConvertTools_ForInvalidTool() {
        var description = "description";
        var name = "name";
        var tool = new UnifiedCompletionRequest.Tool(
            "invalid type",
            new UnifiedCompletionRequest.Tool.FunctionField(description, name, Map.of("key", "value"), null)
        );

        var exception = expectThrows(IllegalArgumentException.class, () -> AmazonBedrockChatCompletionRequest.convertTools(List.of(tool)));

        assertThat(exception.getMessage(), containsString("Unsupported tool type"));
    }

    public void testDetermineToolChoice_ReturnsAutoWhenNull() {
        var result = AmazonBedrockChatCompletionRequest.determineToolChoice(null);
        assertNotNull(result);
        assertThat(result.build(), is(ToolChoice.builder().auto(AutoToolChoice.builder().build()).build()));
    }

    public void testDetermineToolChoice_ReturnsAutoForAutoString() {
        var toolChoice = new UnifiedCompletionRequest.ToolChoiceString(AUTO_TOOL_CHOICE);

        var result = AmazonBedrockChatCompletionRequest.determineToolChoice(toolChoice);

        assertNotNull(result);
        assertThat(result.build(), is(ToolChoice.builder().auto(AutoToolChoice.builder().build()).build()));
    }

    public void testDetermineToolChoice_ReturnsAnyForRequiredString() {
        var toolChoice = new UnifiedCompletionRequest.ToolChoiceString(REQUIRED_TOOL_CHOICE);

        var result = AmazonBedrockChatCompletionRequest.determineToolChoice(toolChoice);

        assertNotNull(result);
        assertThat(result.build(), is(ToolChoice.builder().any(AnyToolChoice.builder().build()).build()));
    }

    public void testDetermineToolChoice_ReturnsNullForNoneString() {
        var toolChoice = new UnifiedCompletionRequest.ToolChoiceString(NONE_TOOL_CHOICE);

        var result = AmazonBedrockChatCompletionRequest.determineToolChoice(toolChoice);

        assertNull(result);
    }

    public void testDetermineToolChoice_ReturnsSpecificToolForObjectChoice() {
        var functionName = "specific_tool";
        var toolChoice = new UnifiedCompletionRequest.ToolChoiceObject(
            FUNCTION_TYPE,
            new UnifiedCompletionRequest.ToolChoiceObject.FunctionField(functionName)
        );

        var result = AmazonBedrockChatCompletionRequest.determineToolChoice(toolChoice);
        assertNotNull(result);
        assertThat(result.build(), is(ToolChoice.builder().tool(SpecificToolChoice.builder().name(functionName).build()).build()));
    }

    public void testDetermineToolChoice_ThrowsForInvalidString() {
        var toolChoice = new UnifiedCompletionRequest.ToolChoiceString("invalid");

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> AmazonBedrockChatCompletionRequest.determineToolChoice(toolChoice)
        );

        assertThat(exception.getMessage(), containsString("Invalid tool choice"));
    }
}
