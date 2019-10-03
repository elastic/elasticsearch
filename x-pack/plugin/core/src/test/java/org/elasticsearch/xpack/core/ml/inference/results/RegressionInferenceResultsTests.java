/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;


public class RegressionInferenceResultsTests extends AbstractWireSerializingTestCase<RegressionInferenceResults> {

    public static RegressionInferenceResults createRandomResults() {
        return new RegressionInferenceResults(randomDouble());
    }

    @Override
    protected RegressionInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<RegressionInferenceResults> instanceReader() {
        return RegressionInferenceResults::new;
    }
}
