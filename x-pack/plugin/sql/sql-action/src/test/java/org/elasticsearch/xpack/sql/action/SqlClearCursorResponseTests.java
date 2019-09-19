/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class SqlClearCursorResponseTests extends AbstractSerializingTestCase<SqlClearCursorResponse> {

    @Override
    protected SqlClearCursorResponse createTestInstance() {
        return new SqlClearCursorResponse(randomBoolean());
    }

    @Override
    protected Writeable.Reader<SqlClearCursorResponse> instanceReader() {
        return SqlClearCursorResponse::new;
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
