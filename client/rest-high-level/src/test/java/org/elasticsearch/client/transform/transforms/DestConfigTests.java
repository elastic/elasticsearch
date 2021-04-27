/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DestConfigTests extends AbstractXContentTestCase<DestConfig> {

    public static DestConfig randomDestConfig() {
        return new DestConfig(randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Override
    protected DestConfig doParseInstance(XContentParser parser) throws IOException {
        return DestConfig.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected DestConfig createTestInstance() {
        return randomDestConfig();
    }

}
