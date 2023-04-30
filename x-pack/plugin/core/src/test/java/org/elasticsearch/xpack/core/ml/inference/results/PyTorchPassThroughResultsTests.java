/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;

import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PyTorchPassThroughResultsTests extends InferenceResultsTestCase<PyTorchPassThroughResults> {

    public static PyTorchPassThroughResults createRandomResults() {
        int rows = randomIntBetween(1, 10);
        int columns = randomIntBetween(1, 10);
        double[][] arr = new double[rows][columns];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                arr[i][j] = randomDouble();
            }
        }

        return new PyTorchPassThroughResults(DEFAULT_RESULTS_FIELD, arr, randomBoolean());
    }

    @Override
    protected Writeable.Reader<PyTorchPassThroughResults> instanceReader() {
        return PyTorchPassThroughResults::new;
    }

    @Override
    protected PyTorchPassThroughResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected PyTorchPassThroughResults mutateInstance(PyTorchPassThroughResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testAsMap() {
        PyTorchPassThroughResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        int size = testInstance.isTruncated ? 2 : 1;
        assertThat(asMap.keySet(), hasSize(size));
        assertArrayEquals(testInstance.getInference(), (double[][]) asMap.get(DEFAULT_RESULTS_FIELD));
        if (testInstance.isTruncated) {
            assertThat(asMap.get("is_truncated"), is(true));
        }
    }

    @Override
    void assertFieldValues(PyTorchPassThroughResults createdInstance, IngestDocument document, String resultsField) {
        assertArrayEquals(
            createdInstance.getInference(),
            document.getFieldValue(resultsField + "." + createdInstance.getResultsField(), double[][].class)
        );
    }
}
