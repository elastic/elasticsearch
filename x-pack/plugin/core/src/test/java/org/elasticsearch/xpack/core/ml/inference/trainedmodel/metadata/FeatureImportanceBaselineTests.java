/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeatureImportanceBaselineTests extends AbstractBWCSerializationTestCase<FeatureImportanceBaseline> {

    private boolean lenient;

    @SuppressWarnings("unchecked")
    public static FeatureImportanceBaseline randomInstance() {
        Supplier<Object> classNameGenerator = randomFrom(
            () -> randomAlphaOfLength(10),
            ESTestCase::randomBoolean,
            () -> randomIntBetween(0, 10)
        );
        return new FeatureImportanceBaseline(
            randomBoolean() ? null : randomDouble(),
            randomBoolean()
                ? null
                : Stream.generate(() -> new FeatureImportanceBaseline.ClassBaseline(classNameGenerator.get(), randomDouble()))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList())
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected FeatureImportanceBaseline createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<FeatureImportanceBaseline> instanceReader() {
        return FeatureImportanceBaseline::new;
    }

    @Override
    protected FeatureImportanceBaseline doParseInstance(XContentParser parser) throws IOException {
        return FeatureImportanceBaseline.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected FeatureImportanceBaseline mutateInstanceForVersion(FeatureImportanceBaseline instance, Version version) {
        return instance;
    }
}
