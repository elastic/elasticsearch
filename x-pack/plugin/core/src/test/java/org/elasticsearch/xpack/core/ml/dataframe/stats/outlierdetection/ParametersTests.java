/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class ParametersTests extends AbstractBWCSerializationTestCase<Parameters> {

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
    protected Parameters mutateInstanceForVersion(Parameters instance, Version version) {
        return instance;
    }

    @Override
    protected Parameters doParseInstance(XContentParser parser) throws IOException {
        return Parameters.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<Parameters> instanceReader() {
        return Parameters::new;
    }

    @Override
    protected Parameters createTestInstance() {
        return createRandom();
    }

    public static Parameters createRandom() {
        int methodsSize = randomIntBetween(1, 5);
        SortedSet<String> methods = new TreeSet<>();
        for (int i = 0; i < methodsSize; i++) {
            methods.add(randomAlphaOfLength(5));
        }

        return new Parameters(
            randomIntBetween(1, Integer.MAX_VALUE),
            methods,
            randomBoolean(),
            randomDouble(),
            randomDouble(),
            randomBoolean()
        );
    }
}
