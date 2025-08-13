/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

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
    protected Parameters mutateInstanceForVersion(Parameters instance, TransportVersion version) {
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

    @Override
    protected Parameters mutateInstance(Parameters instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static Parameters createRandom() {

        return new Parameters(
            randomIntBetween(1, Integer.MAX_VALUE),
            randomAlphaOfLength(5),
            randomBoolean(),
            randomDouble(),
            randomDouble(),
            randomBoolean()
        );
    }
}
