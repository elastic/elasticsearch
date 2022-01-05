/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

public class SqlClearCursorResponseTests extends AbstractSerializingTestCase<SqlClearCursorResponse> {

    @Override
    protected SqlClearCursorResponse createXContextTestInstance(XContentType xContentType) {
        SqlTestUtils.assumeXContentJsonOrCbor(xContentType);
        return super.createXContextTestInstance(xContentType);
    }

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
        org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse response = org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse
            .fromXContent(ProtoShim.toProto(parser));
        return new SqlClearCursorResponse(response.isSucceeded());
    }
}
