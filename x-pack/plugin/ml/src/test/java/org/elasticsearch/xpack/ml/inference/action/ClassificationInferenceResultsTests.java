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

public class ClassificationInferenceResultsTests extends AbstractWireSerializingTestCase<ClassificationInferenceResults> {

    public static ClassificationInferenceResults createRandomResults() {
        return new ClassificationInferenceResults(randomDouble(),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null :
                Stream.generate(ClassificationInferenceResultsTests::createRandomClassEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()));
    }

    private static ClassificationInferenceResults.TopClassEntry createRandomClassEntry() {
        return new ClassificationInferenceResults.TopClassEntry(randomAlphaOfLength(10), randomDouble());
    }

    @Override
    protected ClassificationInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<ClassificationInferenceResults> instanceReader() {
        return ClassificationInferenceResults::new;
    }
}
