/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class InferenceParamsTests extends AbstractWireSerializingTestCase<InferenceParams> {

    public static InferenceParams randomInferenceParams() {
        return randomBoolean() ? InferenceParams.EMPTY_PARAMS : new InferenceParams(randomIntBetween(-1, 100));
    }

    @Override
    protected InferenceParams createTestInstance() {
        return randomInferenceParams();
    }

    @Override
    protected Writeable.Reader<InferenceParams> instanceReader() {
        return InferenceParams::new;
    }

}
