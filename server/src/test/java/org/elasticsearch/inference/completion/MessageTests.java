/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;

import java.util.List;

public class MessageTests extends InferenceObjectRamBytesUsedTest<Message> {

    private static final String CONTENT = "content";
    private static final String ROLE = "role";
    private static final String TOOL_CALL_ID = "tool call id";
    private static final List<ToolCall> TOOL_CALLS = List.of(new ToolCall("id", new ToolCall.FunctionField("args", "name"), "type"));

    @Override
    public Message objectToEstimate() {
        return new Message(new ContentString(CONTENT), ROLE, TOOL_CALL_ID, TOOL_CALLS);
    }

    @Override
    public List<Message> objectsToEstimateWithLargerInput() {
        return List.of(
            // Content larger
            new Message(new ContentString(CONTENT.repeat(10)), ROLE, TOOL_CALL_ID, TOOL_CALLS),
            // Role larger
            new Message(new ContentString(CONTENT), ROLE.repeat(10), TOOL_CALL_ID, TOOL_CALLS),
            // Tool call id larger
            new Message(new ContentString(CONTENT), ROLE, TOOL_CALL_ID.repeat(10), TOOL_CALLS),
            // More tool calls
            new Message(new ContentString(CONTENT), ROLE, TOOL_CALL_ID, List.of(TOOL_CALLS.getFirst(), TOOL_CALLS.getFirst()))
        );
    }
}
