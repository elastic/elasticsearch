/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class SqlClearCursorRequestTests extends AbstractStreamableXContentTestCase<SqlClearCursorRequest> {

    @Override
    protected SqlClearCursorRequest createTestInstance() {
        return new SqlClearCursorRequest(randomAlphaOfLength(100));
    }

    @Override
    protected SqlClearCursorRequest createBlankInstance() {
        return new SqlClearCursorRequest();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MutateFunction<SqlClearCursorRequest> getMutateFunction() {
        return request -> getCopyFunction().copy(request).setCursor(randomAlphaOfLength(100));
    }

    @Override
    protected SqlClearCursorRequest doParseInstance(XContentParser parser) {
        return SqlClearCursorRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}

