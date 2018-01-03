/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class SqlListTablesRequestTests extends AbstractSerializingTestCase<SqlListTablesRequest> {
    @Override
    protected SqlListTablesRequest createTestInstance() {
        return new SqlListTablesRequest(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<SqlListTablesRequest> instanceReader() {
        return SqlListTablesRequest::new;
    }

    @Override
    protected SqlListTablesRequest doParseInstance(XContentParser parser) throws IOException {
        return SqlListTablesRequest.fromXContent(parser);
    }
}
