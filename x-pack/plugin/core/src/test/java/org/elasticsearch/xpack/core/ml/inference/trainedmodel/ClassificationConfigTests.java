/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class ClassificationConfigTests extends AbstractBWCSerializationTestCase<ClassificationConfig> {

    private boolean lenient;

    public static ClassificationConfig randomClassificationConfig() {
        return new ClassificationConfig(
            randomBoolean() ? null : randomIntBetween(-1, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomFrom(PredictionFieldType.values())
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected ClassificationConfig createTestInstance() {
        return randomClassificationConfig();
    }

    @Override
    protected ClassificationConfig mutateInstance(ClassificationConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<ClassificationConfig> instanceReader() {
        return ClassificationConfig::new;
    }

    @Override
    protected ClassificationConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? ClassificationConfig.fromXContentLenient(parser) : ClassificationConfig.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected ClassificationConfig mutateInstanceForVersion(ClassificationConfig instance, TransportVersion version) {
        return instance;
    }
}
