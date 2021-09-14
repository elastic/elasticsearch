/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class ReadOnlyActionTests extends AbstractXContentTestCase<ReadOnlyAction> {

    @Override
    protected ReadOnlyAction doParseInstance(XContentParser parser) {
        return ReadOnlyAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ReadOnlyAction createTestInstance() {
        return new ReadOnlyAction();
    }
}
