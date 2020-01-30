/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class RawInferenceResultsTests extends AbstractWireSerializingTestCase<RawInferenceResults> {

    public static RawInferenceResults createRandomResults() {
        return new RawInferenceResults(randomDouble());
    }

    @Override
    protected RawInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<RawInferenceResults> instanceReader() {
        return RawInferenceResults::new;
    }
}
