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

public class FunctionFieldTests extends InferenceObjectRamBytesUsedTest<Tool.FunctionField> {
    private static final String DESCRIPTION = "description";
    private static final String NAME = "name";
    private static final String KEY_ONE = "key 1";
    private static final String VALUE_ONE = "value 1";
    private static final Map<String, Object> PARAMETERS = Map.of(KEY_ONE, VALUE_ONE);
    private static final Boolean STRICT = true;

    @Override
    public Tool.FunctionField objectToEstimate() {
        return new Tool.FunctionField(DESCRIPTION, NAME, PARAMETERS, STRICT);
    }

    @Override
    public List<Tool.FunctionField> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger description
            new Tool.FunctionField(DESCRIPTION.repeat(5), NAME, PARAMETERS, STRICT),
            // Larger name
            new Tool.FunctionField(DESCRIPTION, NAME.repeat(5), PARAMETERS, STRICT),
            // More parameters
            new Tool.FunctionField(DESCRIPTION, NAME, Map.of(KEY_ONE, VALUE_ONE, "key two", "value two"), STRICT)
        );
    }
}
