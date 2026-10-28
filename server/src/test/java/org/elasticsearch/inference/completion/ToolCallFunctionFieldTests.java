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

public class ToolCallFunctionFieldTests extends InferenceObjectRamBytesUsedTest<ToolCall.FunctionField> {

    private static final String ARGUMENTS = "arguments";
    private static final String NAME = "name";

    @Override
    public ToolCall.FunctionField objectToEstimate() {
        return new ToolCall.FunctionField(ARGUMENTS, NAME);
    }

    @Override
    public List<ToolCall.FunctionField> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger arguments
            new ToolCall.FunctionField(ARGUMENTS.repeat(5), NAME),
            // Larger name
            new ToolCall.FunctionField(ARGUMENTS, NAME.repeat(5))
        );
    }
}
