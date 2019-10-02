/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferenceResultsTests extends AbstractWireSerializingTestCase<InferenceResults> {

    public static InferenceResults createRandomResults() {
        return new InferenceResults(randomDouble(),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null :
                Stream.generate(InferenceResultsTests::createRandomClassEntry).limit(randomIntBetween(0, 10)).collect(Collectors.toList()));
    }

    private static InferenceResults.TopClassEntry createRandomClassEntry() {
        return new InferenceResults.TopClassEntry(randomAlphaOfLength(10), randomDouble());
    }

    @Override
    protected InferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<InferenceResults> instanceReader() {
        return InferenceResults::new;
    }
}
