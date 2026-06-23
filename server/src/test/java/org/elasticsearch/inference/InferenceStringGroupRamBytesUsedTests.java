/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import java.util.List;

public class InferenceStringGroupRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<InferenceStringGroup> {

    private static final InferenceString INFERENCE_STRING = new InferenceString(DataType.TEXT, "value");

    @Override
    public InferenceStringGroup objectToEstimate() {
        return new InferenceStringGroup(List.of(INFERENCE_STRING));
    }

    @Override
    public List<InferenceStringGroup> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger value
            new InferenceStringGroup(List.of(new InferenceString(DataType.TEXT, INFERENCE_STRING.value().repeat(5)))),
            // More values
            new InferenceStringGroup(
                List.of(
                    new InferenceString(DataType.TEXT, INFERENCE_STRING.value()),
                    new InferenceString(DataType.TEXT, INFERENCE_STRING.value())
                )
            )
        );
    }
}
