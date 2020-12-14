/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;


public class HyperparameterImportanceTests extends AbstractBWCSerializationTestCase<HyperparameterImportance> {

    private boolean lenient;

    @SuppressWarnings("unchecked")
    public static HyperparameterImportance randomInstance() {
        return new HyperparameterImportance(
            randomAlphaOfLength(10),
            randomDoubleBetween(0.0, 1.0, true),
            randomDoubleBetween(0.0, 100.0, true),
            randomDoubleBetween(0.0, 1.0, true));
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected HyperparameterImportance createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<HyperparameterImportance> instanceReader() {
        return HyperparameterImportance::new;
    }

    @Override
    protected HyperparameterImportance doParseInstance(XContentParser parser) throws IOException {
        return HyperparameterImportance.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected HyperparameterImportance mutateInstanceForVersion(HyperparameterImportance instance, Version version) {
        return instance;
    }
}
