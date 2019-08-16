/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class AnalyticsResultTests extends AbstractXContentTestCase<AnalyticsResult> {

    @Override
    protected AnalyticsResult createTestInstance() {
        RowResults rowResults = null;
        Integer progressPercent = null;
        if (randomBoolean()) {
            rowResults = RowResultsTests.createRandom();
        }
        if (randomBoolean()) {
            progressPercent = randomIntBetween(0, 100);
        }
        return new AnalyticsResult(rowResults, progressPercent);
    }

    @Override
    protected AnalyticsResult doParseInstance(XContentParser parser) throws IOException {
        return AnalyticsResult.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
