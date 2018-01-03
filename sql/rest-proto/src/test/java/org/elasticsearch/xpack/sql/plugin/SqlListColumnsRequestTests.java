/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class SqlListColumnsRequestTests extends AbstractSerializingTestCase<SqlListColumnsRequest> {
    @Override
    protected SqlListColumnsRequest createTestInstance() {
        return new SqlListColumnsRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<SqlListColumnsRequest> instanceReader() {
        return SqlListColumnsRequest::new;
    }

    @Override
    protected SqlListColumnsRequest doParseInstance(XContentParser parser) throws IOException {
        return SqlListColumnsRequest.fromXContent(parser);
    }
}
