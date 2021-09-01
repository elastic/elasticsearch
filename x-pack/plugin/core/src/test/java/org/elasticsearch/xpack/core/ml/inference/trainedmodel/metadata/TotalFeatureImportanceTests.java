/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TotalFeatureImportanceTests extends AbstractBWCSerializationTestCase<TotalFeatureImportance> {

    private boolean lenient;

    @SuppressWarnings("unchecked")
    public static TotalFeatureImportance randomInstance() {
        Supplier<Object> classNameGenerator = randomFrom(
            () -> randomAlphaOfLength(10),
            ESTestCase::randomBoolean,
            () -> randomIntBetween(0, 10)
        );
        return new TotalFeatureImportance(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomImportance(),
            randomBoolean() ?
                null :
                Stream.generate(() -> new TotalFeatureImportance.ClassImportance(classNameGenerator.get(), randomImportance()))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toList())
            );
    }

    private static TotalFeatureImportance.Importance randomImportance() {
        return new TotalFeatureImportance.Importance(randomDouble(), randomDouble(), randomDouble());
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TotalFeatureImportance createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<TotalFeatureImportance> instanceReader() {
        return TotalFeatureImportance::new;
    }

    @Override
    protected TotalFeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return TotalFeatureImportance.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected TotalFeatureImportance mutateInstanceForVersion(TotalFeatureImportance instance, Version version) {
        return instance;
    }
}
