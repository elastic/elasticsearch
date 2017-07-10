/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.cli.net.protocol.CliRoundTripTestUtils.assertRoundTripCurrentVersion;

public class ExceptionResponseTests extends ESTestCase {
    static ExceptionResponse randomExceptionResponse() {
        return new ExceptionResponse(randomFrom(RequestType.values()), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomExceptionResponse());
    }

    public void testToString() {
        assertEquals("ExceptionResponse<request=[COMMAND] message=[test] cause=[test]>",
                new ExceptionResponse(RequestType.COMMAND, "test", "test").toString());
    }
}
