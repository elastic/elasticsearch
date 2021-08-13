/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.stats;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class SimpleStatsTests extends AbstractXContentTestCase<SimpleStats> {

    @Override
    protected SimpleStats createTestInstance() {
        return createRandom();
    }

    @Override
    protected SimpleStats doParseInstance(XContentParser parser) throws IOException {
        return SimpleStats.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static SimpleStats createRandom() {
        return new SimpleStats(randomDouble(), randomDouble(), randomDouble(), randomDouble());
    }
}
