/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.regression;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class HyperparametersTests extends AbstractXContentTestCase<Hyperparameters> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Hyperparameters doParseInstance(XContentParser parser) throws IOException {
        return Hyperparameters.PARSER.apply(parser, null);
    }


    @Override
    protected Hyperparameters createTestInstance() {
        return createRandom();
    }

    public static Hyperparameters createRandom() {
        return new Hyperparameters(
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? null : randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble()
        );
    }
}
