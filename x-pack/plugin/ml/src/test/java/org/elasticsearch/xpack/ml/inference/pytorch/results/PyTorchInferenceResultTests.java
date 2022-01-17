/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PyTorchInferenceResultTests extends AbstractXContentTestCase<PyTorchInferenceResult> {

    @Override
    protected PyTorchInferenceResult doParseInstance(XContentParser parser) throws IOException {
        return PyTorchInferenceResult.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PyTorchInferenceResult createTestInstance() {
        return createRandom();
    }

    public static PyTorchInferenceResult createRandom() {
        boolean createError = randomBoolean();
        String id = randomAlphaOfLength(6);
        if (createError) {
            return new PyTorchInferenceResult(id, null, null, "This is an error message");
        } else {
            int rows = randomIntBetween(1, 10);
            int columns = randomIntBetween(1, 10);
            int depth = randomIntBetween(1, 10);
            double[][][] arr = new double[rows][columns][depth];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < columns; j++) {
                    for (int k = 0; k < depth; k++) {
                        arr[i][j][k] = randomDouble();
                    }
                }
            }
            return new PyTorchInferenceResult(id, arr, randomLong(), null);
        }
    }
}
