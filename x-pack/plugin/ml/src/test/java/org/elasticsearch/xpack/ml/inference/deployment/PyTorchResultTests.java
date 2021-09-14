/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class PyTorchResultTests extends AbstractSerializingTestCase<PyTorchResult> {
    @Override
    protected PyTorchResult doParseInstance(XContentParser parser) throws IOException {
        return PyTorchResult.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<PyTorchResult> instanceReader() {
        return PyTorchResult::new;
    }

    @Override
    protected PyTorchResult createTestInstance() {
        boolean createError = randomBoolean();
        String id = randomAlphaOfLength(6);
        if (createError) {
            return new PyTorchResult(id, null, null, "This is an error message");
        } else {
            int rows = randomIntBetween(1, 10);
            int columns = randomIntBetween(1, 10);
            int depth = randomIntBetween(1, 10);
            double [][][] arr = new double[rows][columns][depth];
            for (int i=0; i<rows; i++) {
                for (int j=0; j<columns; j++) {
                    for (int k=0; k<depth; k++) {
                        arr[i][j][k] = randomDouble();
                    }
                }
            }
            return new PyTorchResult(id, arr, randomLong(), null);
        }
    }
}
