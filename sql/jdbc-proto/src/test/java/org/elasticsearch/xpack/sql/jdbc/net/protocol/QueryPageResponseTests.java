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
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequestTests.randomQueryPageRequest;

public class QueryPageResponseTests extends ESTestCase {
    static QueryPageResponse randomQueryPageResponse(Page page) {
        return new QueryPageResponse(randomAlphaOfLength(5), page);
    }

    public void testRoundTrip() throws IOException {
        Page page = randomPage();
        assertRoundTripCurrentVersion(() -> randomQueryPageRequest(new Page(page.columnInfo())), randomQueryPageResponse(page));
    }

    public void testToString() {
        Page results = new Page(singletonList(varcharInfo("a")), new Object[][] {
                new Object[] {"test"}
        });
        assertEquals("QueryPageResponse<requestId=[test_id] data=[\ntest\n]>", new QueryPageResponse("test_id", results).toString());
    }
}
