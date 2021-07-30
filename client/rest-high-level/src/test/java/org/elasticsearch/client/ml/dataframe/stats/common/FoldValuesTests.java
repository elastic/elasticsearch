/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class FoldValuesTests extends AbstractXContentTestCase<FoldValues> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected FoldValues doParseInstance(XContentParser parser) throws IOException {
        return FoldValues.PARSER.apply(parser, null);
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
}
