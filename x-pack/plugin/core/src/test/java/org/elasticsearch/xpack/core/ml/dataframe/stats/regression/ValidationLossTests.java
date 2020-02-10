/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.FoldValuesTests;
import org.junit.Before;

import java.io.IOException;

public class ValidationLossTests extends AbstractBWCSerializationTestCase<ValidationLoss> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected ValidationLoss doParseInstance(XContentParser parser) throws IOException {
        return ValidationLoss.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<ValidationLoss> instanceReader() {
        return ValidationLoss::new;
    }

    @Override
    protected ValidationLoss createTestInstance() {
        return createRandom();
    }

    public static ValidationLoss createRandom() {
        return new ValidationLoss(
            randomAlphaOfLength(10),
            randomList(5, () -> FoldValuesTests.createRandom())
        );
    }

    @Override
    protected ValidationLoss mutateInstanceForVersion(ValidationLoss instance, Version version) {
        return instance;
    }
}
