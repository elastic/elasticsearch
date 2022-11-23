/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ThreadSettingsTests extends AbstractXContentTestCase<ThreadSettings> {

    public static ThreadSettings createRandom() {
        return new ThreadSettings(randomIntBetween(1, Integer.MAX_VALUE), randomIntBetween(1, Integer.MAX_VALUE));
    }

    @Override
    protected ThreadSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ThreadSettings doParseInstance(XContentParser parser) throws IOException {
        return ThreadSettings.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
