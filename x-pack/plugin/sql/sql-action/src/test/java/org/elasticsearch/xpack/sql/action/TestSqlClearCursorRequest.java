/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.proto.Payloads;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;

/**
 * Extension of SqlClearCursorRequest that relies on the sql-proto non-X-Content serialization
 * and then wrapping the outputstream in builder.
 */
public class TestSqlClearCursorRequest extends SqlClearCursorRequest implements ToXContentObject {

    public TestSqlClearCursorRequest(StreamInput in) throws IOException {
        super(in);
    }

    public TestSqlClearCursorRequest(RequestInfo requestInfo, String cursor) {
        super(requestInfo, cursor);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        org.elasticsearch.xpack.sql.proto.SqlClearCursorRequest protoInstance = new org.elasticsearch.xpack.sql.proto.SqlClearCursorRequest(
            this.getCursor(),
            this.requestInfo()
        );
        return SqlTestUtils.toXContentBuilder(builder, g -> Payloads.generate(g, protoInstance));
    }
}
