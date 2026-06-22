/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;

import java.util.List;

public class UnifiedCompletionRequestTests extends InferenceObjectRamBytesUsedTest<UnifiedCompletionRequest> {

    private static final List<Message> MESSAGES = List.of(new Message(new ContentString("content"), "role", "id", null));
    private static final String MODEL = "model";
    private static final List<String> STOP = List.of("stop");
    private static final ToolChoice TOOL_CHOICE = new ToolChoice.ToolChoiceString("value");
    private static final List<Tool> TOOLS = List.of(new Tool("type", new Tool.FunctionField("description", "name", null, null)));

    @Override
    public UnifiedCompletionRequest objectToEstimate() {
        return new UnifiedCompletionRequest(MESSAGES, MODEL, null, STOP, null, TOOL_CHOICE, TOOLS, null);
    }

    @Override
    public List<UnifiedCompletionRequest> objectsToEstimateWithLargerInput() {
        return List.of(
            // More messages
            new UnifiedCompletionRequest(List.of(MESSAGES.getFirst(), MESSAGES.getFirst()), MODEL, null, STOP, null, TOOL_CHOICE, TOOLS, null),
            // Longer model
            new UnifiedCompletionRequest(MESSAGES, MODEL.repeat(5), null, STOP, null, TOOL_CHOICE, TOOLS, null),
            // More stop
            new UnifiedCompletionRequest(MESSAGES, MODEL, null, List.of(STOP.getFirst(), STOP.getFirst()), null, TOOL_CHOICE, TOOLS, null),
            // More tools
            new UnifiedCompletionRequest(MESSAGES, MODEL, null, STOP, null, TOOL_CHOICE, List.of(TOOLS.getFirst(), TOOLS.getFirst()), null)
        );
    }
}
