/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ClassificationFeatureImportanceTests extends AbstractXContentSerializingTestCase<ClassificationFeatureImportance> {

    @Override
    protected ClassificationFeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return ClassificationFeatureImportance.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ClassificationFeatureImportance> instanceReader() {
        return ClassificationFeatureImportance::new;
    }

    @Override
    protected ClassificationFeatureImportance createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected ClassificationFeatureImportance mutateInstance(ClassificationFeatureImportance instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @SuppressWarnings("unchecked")
    public static ClassificationFeatureImportance createRandomInstance() {
        Supplier<Object> classNameGenerator = randomFrom(
            () -> randomAlphaOfLength(10),
            ESTestCase::randomBoolean,
            () -> randomIntBetween(0, 10)
        );
        return new ClassificationFeatureImportance(
            randomAlphaOfLength(10),
            Stream.generate(classNameGenerator)
                .limit(randomLongBetween(2, 10))
                .map(name -> new ClassificationFeatureImportance.ClassImportance(name, randomDoubleBetween(-10, 10, false)))
                .collect(Collectors.toList())
        );
    }

    public void testGetTotalImportance_GivenBinary() {
        ClassificationFeatureImportance featureImportance = new ClassificationFeatureImportance(
            "binary",
            Arrays.asList(
                new ClassificationFeatureImportance.ClassImportance("a", 0.15),
                new ClassificationFeatureImportance.ClassImportance("not-a", -0.15)
            )
        );

        assertThat(featureImportance.getTotalImportance(), equalTo(0.15));
    }

    public void testGetTotalImportance_GivenMulticlass() {
        ClassificationFeatureImportance featureImportance = new ClassificationFeatureImportance(
            "multiclass",
            Arrays.asList(
                new ClassificationFeatureImportance.ClassImportance("a", 0.15),
                new ClassificationFeatureImportance.ClassImportance("b", -0.05),
                new ClassificationFeatureImportance.ClassImportance("c", 0.30)
            )
        );

        assertThat(featureImportance.getTotalImportance(), closeTo(0.50, 0.00000001));
    }
}
