/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class FoldValuesTests extends AbstractBWCSerializationTestCase<FoldValues> {

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
    protected FoldValues doParseInstance(XContentParser parser) throws IOException {
        return FoldValues.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<FoldValues> instanceReader() {
        return FoldValues::new;
    }

    @Override
    protected FoldValues createTestInstance() {
        return createRandom();
    }

    public static FoldValues createRandom() {
        int valuesSize = randomIntBetween(0, 10);
        double[] values = new double[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            values[i] = randomDouble();
        }
        return new FoldValues(randomIntBetween(0, Integer.MAX_VALUE), values);
    }

    @Override
    protected FoldValues mutateInstanceForVersion(FoldValues instance, Version version) {
        return instance;
    }
}
