/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.regression;

import org.elasticsearch.client.ml.dataframe.stats.common.FoldValuesTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class ValidationLossTests extends AbstractXContentTestCase<ValidationLoss> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ValidationLoss doParseInstance(XContentParser parser) throws IOException {
        return ValidationLoss.PARSER.apply(parser, null);
    }

    @Override
    protected ValidationLoss createTestInstance() {
        return createRandom();
    }

    public static ValidationLoss createRandom() {
        return new ValidationLoss(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomList(5, () -> FoldValuesTests.createRandom())
        );
    }
}
