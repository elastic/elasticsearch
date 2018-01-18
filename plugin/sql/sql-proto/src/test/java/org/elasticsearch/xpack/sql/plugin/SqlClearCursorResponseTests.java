/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

public class SqlClearCursorResponseTests extends AbstractStreamableXContentTestCase<SqlClearCursorResponse> {

    @Override
    protected SqlClearCursorResponse createTestInstance() {
        return new SqlClearCursorResponse(randomBoolean());
    }

    @Override
    protected SqlClearCursorResponse createBlankInstance() {
        return new SqlClearCursorResponse();
    }

    @Override
    protected MutateFunction<SqlClearCursorResponse> getMutateFunction() {
        return response -> getCopyFunction().copy(response).setSucceeded(response.isSucceeded() == false);
    }

    @Override
    protected SqlClearCursorResponse doParseInstance(XContentParser parser) {
        return SqlClearCursorResponse.fromXContent(parser);
    }
}
