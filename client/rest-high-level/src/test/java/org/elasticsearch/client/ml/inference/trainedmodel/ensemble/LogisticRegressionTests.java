/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LogisticRegressionTests extends AbstractXContentTestCase<LogisticRegression> {

    LogisticRegression createTestInstance(int numberOfWeights) {
        return new LogisticRegression(Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).collect(Collectors.toList()));
    }

    @Override
    protected LogisticRegression doParseInstance(XContentParser parser) throws IOException {
        return LogisticRegression.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected LogisticRegression createTestInstance() {
        return randomBoolean() ? new LogisticRegression(null) : createTestInstance(randomIntBetween(1, 100));
    }

}
