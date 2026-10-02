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
import java.util.Map;

public class ToolTests extends InferenceObjectRamBytesUsedTest<Tool> {

    private static final String TYPE = "type";
    private static final Tool.FunctionField FUNCTION_FIELD = new Tool.FunctionField("description", "name", null, true);

    @Override
    public Tool objectToEstimate() {
        return new Tool(TYPE, FUNCTION_FIELD);
    }

    @Override
    public List<Tool> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger type string
            new Tool(TYPE.repeat(10), FUNCTION_FIELD),
            // Function field with parameters
            new Tool(TYPE, new Tool.FunctionField(FUNCTION_FIELD.description(), FUNCTION_FIELD.name(), Map.of("Key", "Value"), false))
        );
    }
}
