/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class SqlListTablesResponseTests extends AbstractStreamableXContentTestCase<SqlListTablesResponse> {

    @Override
    protected SqlListTablesResponse createTestInstance() {
        int tableCount = between(1, 10);

        List<String> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++) {
            tables.add(randomAlphaOfLength(10));
        }
        return new SqlListTablesResponse(tables);
    }

    @Override
    protected SqlListTablesResponse createBlankInstance() {
        return new SqlListTablesResponse();
    }

    @Override
    protected SqlListTablesResponse doParseInstance(XContentParser parser) {
        return SqlListTablesResponse.fromXContent(parser);
    }
}
