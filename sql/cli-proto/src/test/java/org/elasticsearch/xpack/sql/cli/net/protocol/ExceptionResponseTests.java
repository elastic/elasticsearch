/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.cli.net.protocol.CliRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitRequestTests.randomQueryInitRequest;


public class ExceptionResponseTests extends ESTestCase {
    static ExceptionResponse randomExceptionResponse() {
        return new ExceptionResponse(RequestType.QUERY_INIT, randomAlphaOfLength(5), randomAlphaOfLength(5),
                randomFrom(SqlExceptionType.values()));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomQueryInitRequest(), randomExceptionResponse());
    }

    public void testToString() {
        assertEquals("ExceptionResponse<request=[QUERY_INIT] message=[test] cause=[test] type=[SYNTAX]>",
                new ExceptionResponse(RequestType.QUERY_INIT, "test", "test", SqlExceptionType.SYNTAX).toString());
    }
}
