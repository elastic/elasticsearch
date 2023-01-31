/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class HyperparametersTests extends AbstractBWCSerializationTestCase<Hyperparameters> {

    private boolean lenient;

    @SuppressWarnings("unchecked")
    public static Hyperparameters randomInstance() {
        boolean supplied = randomBoolean();
        return new Hyperparameters(
            randomAlphaOfLength(10),
            randomDoubleBetween(0.0, 1.0, true),
            // If supplied, the importance values are possibly nullable
            supplied && randomBoolean() ? null : randomDoubleBetween(0.0, 100.0, true),
            supplied && randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true),
            supplied
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected Hyperparameters createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Hyperparameters mutateInstance(Hyperparameters instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Hyperparameters> instanceReader() {
        return Hyperparameters::new;
    }

    @Override
    protected Hyperparameters doParseInstance(XContentParser parser) throws IOException {
        return Hyperparameters.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Hyperparameters mutateInstanceForVersion(Hyperparameters instance, Version version) {
        return instance;
    }
}
