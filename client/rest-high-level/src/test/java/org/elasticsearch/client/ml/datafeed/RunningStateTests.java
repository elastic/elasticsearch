/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RunningStateTests extends AbstractXContentTestCase<RunningState> {

    public static RunningState createRandomInstance() {
        return new RunningState(randomBoolean(), randomBoolean());
    }

    @Override
    protected RunningState createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected RunningState doParseInstance(XContentParser parser) throws IOException {
        return RunningState.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
