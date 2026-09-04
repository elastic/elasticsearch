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

public class ToolChoiceObjectFunctionFieldTests extends InferenceObjectRamBytesUsedTest<ToolChoice.ToolChoiceObject.FunctionField> {

    private static final String NAME = "name";

    @Override
    public ToolChoice.ToolChoiceObject.FunctionField objectToEstimate() {
        return new ToolChoice.ToolChoiceObject.FunctionField(NAME);
    }

    @Override
    public List<ToolChoice.ToolChoiceObject.FunctionField> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger name
            new ToolChoice.ToolChoiceObject.FunctionField(NAME.repeat(5))
        );
    }
}
