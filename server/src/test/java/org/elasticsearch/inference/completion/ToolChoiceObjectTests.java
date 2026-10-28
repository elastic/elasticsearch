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

public class ToolChoiceObjectTests extends InferenceObjectRamBytesUsedTest<ToolChoice.ToolChoiceObject> {

    private static final String TYPE = "type";
    private static final ToolChoice.ToolChoiceObject.FunctionField FUNCTION_FIELD = new ToolChoice.ToolChoiceObject.FunctionField("name");

    @Override
    public ToolChoice.ToolChoiceObject objectToEstimate() {
        return new ToolChoice.ToolChoiceObject(TYPE, FUNCTION_FIELD);
    }

    @Override
    public List<ToolChoice.ToolChoiceObject> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger type
            new ToolChoice.ToolChoiceObject(TYPE.repeat(5), FUNCTION_FIELD)
        );
    }
}
