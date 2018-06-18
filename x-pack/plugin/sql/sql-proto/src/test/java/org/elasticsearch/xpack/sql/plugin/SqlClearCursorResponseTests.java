/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

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
    protected SqlClearCursorResponse mutateInstance(SqlClearCursorResponse instance) {
        return new SqlClearCursorResponse(instance.isSucceeded() == false);
    }

    @Override
    protected SqlClearCursorResponse doParseInstance(XContentParser parser) {
        org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse response =
            org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse.fromXContent(parser);
        return new SqlClearCursorResponse(response.isSucceeded());
    }
}
