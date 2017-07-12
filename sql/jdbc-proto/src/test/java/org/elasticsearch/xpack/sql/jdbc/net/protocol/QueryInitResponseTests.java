/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfoTests.varcharInfo;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.PageTests.randomPage;

public class QueryInitResponseTests extends ESTestCase {
    static QueryInitResponse randomQueryInitResponse() {
        Page page = randomPage();
        return new QueryInitResponse(randomNonNegativeLong(), randomNonNegativeLong(), randomAlphaOfLength(5), page.columnInfo(), page);
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(QueryInitRequestTests::randomQueryInitRequest, randomQueryInitResponse());
    }

    public void testToString() {
        Page page = new Page(singletonList(varcharInfo("a")), new Object[][] {
                new Object[] {"test"},
                new Object[] {"string"},
        });
        assertEquals("QueryInitResponse<timeReceived=[123] timeSent=[456] requestId=[test_id] columns=[a<type=[VARCHAR]>] data=["
                    + "\ntest\nstring\n]>",
                new QueryInitResponse(123, 456, "test_id", page.columnInfo(), page).toString());
    }
}
