/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class PyTorchPassThroughResultsTests extends AbstractWireSerializingTestCase<PyTorchPassThroughResults> {
    @Override
    protected Writeable.Reader<PyTorchPassThroughResults> instanceReader() {
        return PyTorchPassThroughResults::new;
    }

    @Override
    protected PyTorchPassThroughResults createTestInstance() {
        int rows = randomIntBetween(1, 10);
        int columns = randomIntBetween(1, 10);
        double [][] arr = new double[rows][columns];
        for (int i=0; i<rows; i++) {
            for (int j=0; j<columns; j++) {
                arr[i][j] = randomDouble();
            }
        }

        return new PyTorchPassThroughResults(arr);
    }

    public void testAsMap() {
        PyTorchPassThroughResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.keySet(), hasSize(1));
        assertArrayEquals(testInstance.getInference(), (double[][]) asMap.get(PyTorchPassThroughResults.DEFAULT_RESULTS_FIELD));
    }
}
