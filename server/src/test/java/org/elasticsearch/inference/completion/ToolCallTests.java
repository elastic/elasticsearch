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

public class ToolCallTests extends InferenceObjectRamBytesUsedTest<ToolCall> {

    private static final String ID = "id";
    private static final ToolCall.FunctionField FUNCTION_FIELD = new ToolCall.FunctionField("arguments", "name");
    private static final String TYPE = "type";

    @Override
    public ToolCall objectToEstimate() {
        return new ToolCall(ID, FUNCTION_FIELD, TYPE);
    }

    @Override
    public List<ToolCall> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger id
            new ToolCall(ID.repeat(5), FUNCTION_FIELD, TYPE),
            // Larger type
            new ToolCall(ID, FUNCTION_FIELD, TYPE.repeat(5))
        );
    }
}
