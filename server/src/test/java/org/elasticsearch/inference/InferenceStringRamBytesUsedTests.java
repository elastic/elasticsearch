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

public class InferenceStringRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<InferenceString> {

    private static final String VALUE = "value";

    @Override
    public InferenceString objectToEstimate() {
        return new InferenceString(DataType.TEXT, DataFormat.TEXT, VALUE);
    }

    @Override
    public List<InferenceString> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger value
            new InferenceString(DataType.TEXT, DataFormat.TEXT, VALUE.repeat(5))
        );
    }
}
