/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.JdbcRoundTripTestUtils.assertRoundTripCurrentVersion;

public class ExceptionResponseTests extends ESTestCase {
    static ExceptionResponse randomExceptionResponse() {
        return new ExceptionResponse(RequestType.META_TABLE, randomAlphaOfLength(5), randomAlphaOfLength(5),
                randomFrom(SqlExceptionType.values()));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(MetaTableRequestTests::randomMetaTableRequest, randomExceptionResponse());
    }

    public void testToString() {
        assertEquals("ExceptionResponse<request=[INFO] message=[test] cause=[test] type=[SYNTAX]>",
                new ExceptionResponse(RequestType.INFO, "test", "test", SqlExceptionType.SYNTAX).toString());
    }
}
