/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResultsTests;

import java.io.IOException;

public class AnalyticsResultTests extends AbstractXContentTestCase<AnalyticsResult> {

    @Override
    protected AnalyticsResult createTestInstance() {
        RowResults rowResults = null;
        if (randomBoolean()) {
            rowResults = RowResultsTests.createRandom();
        }
        return new AnalyticsResult(rowResults);
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
