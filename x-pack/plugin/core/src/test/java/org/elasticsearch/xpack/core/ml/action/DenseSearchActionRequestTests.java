/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdateTests;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class DenseSearchActionRequestTests extends AbstractWireSerializingTestCase<DenseSearchAction.Request> {
    @Override
    protected Writeable.Reader<DenseSearchAction.Request> instanceReader() {
        return DenseSearchAction.Request::new;
    }

    @Override
    protected DenseSearchAction.Request createTestInstance() {
        return new DenseSearchAction.Request(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomKnnSearchBuilder(),
            TextEmbeddingConfigUpdateTests.randomUpdate(),
            TimeValue.timeValueSeconds(randomIntBetween(1, 10))
        );
    }

    private KnnSearchBuilder randomKnnSearchBuilder() {
        String field = randomAlphaOfLength(6);
        float[] vector = new float[] { randomFloat(), randomFloat(), randomFloat() };
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k + 20, 1000);
        return new KnnSearchBuilder(field, vector, k, numCands);
    }

    public void testValidate() {
        var validAction = createTestInstance();
        assertNull(validAction.validate());

        var action = new DenseSearchAction.Request(null, null, null, null, null);
        var validation = action.validate();
        assertNotNull(validation);
        assertThat(validation.validationErrors(), hasSize(3));
        assertThat(validation.validationErrors().get(0), containsString("query_string cannot be null"));
        assertThat(validation.validationErrors().get(1), containsString("deployment_id cannot be null"));
        assertThat(validation.validationErrors().get(2), containsString("knn cannot be null"));
    }
}
