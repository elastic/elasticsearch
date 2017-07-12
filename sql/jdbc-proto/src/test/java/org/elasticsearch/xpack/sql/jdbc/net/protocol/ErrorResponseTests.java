/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class ErrorResponseTests extends ESTestCase {
    static ErrorResponse randomErrorResponse() {
        return new ErrorResponse(RequestType.META_TABLE, randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(MetaTableRequestTests::randomMetaTableRequest, randomErrorResponse());
    }

    public void testToString() {
        assertEquals("ErrorResponse<request=[INFO] message=[test] cause=[test] stack=[stack\nstack]>",
                new ErrorResponse(RequestType.INFO, "test", "test", "stack\nstack").toString());
    }
}
