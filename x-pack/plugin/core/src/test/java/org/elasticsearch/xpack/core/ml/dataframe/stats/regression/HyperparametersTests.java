/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class HyperparametersTests extends AbstractBWCSerializationTestCase<Hyperparameters> {

    private boolean lenient;

    @Before
    public void chooseLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Hyperparameters mutateInstanceForVersion(Hyperparameters instance, Version version) {
        return instance;
    }

    @Override
    protected Hyperparameters doParseInstance(XContentParser parser) throws IOException {
        return Hyperparameters.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<Hyperparameters> instanceReader() {
        return Hyperparameters::new;
    }

    @Override
    protected Hyperparameters createTestInstance() {
        return createRandom();
    }

    public static Hyperparameters createRandom() {
        return new Hyperparameters(
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomDouble(),
            randomDouble()
        );
    }
}
