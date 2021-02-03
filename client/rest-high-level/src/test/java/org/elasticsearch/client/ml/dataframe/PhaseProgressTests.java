/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PhaseProgressTests extends AbstractXContentTestCase<PhaseProgress> {

    public static PhaseProgress createRandom() {
        return new PhaseProgress(randomAlphaOfLength(20), randomIntBetween(0, 100));
    }

    @Override
    protected PhaseProgress createTestInstance() {
        return createRandom();
    }

    @Override
    protected PhaseProgress doParseInstance(XContentParser parser) throws IOException {
        return PhaseProgress.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
