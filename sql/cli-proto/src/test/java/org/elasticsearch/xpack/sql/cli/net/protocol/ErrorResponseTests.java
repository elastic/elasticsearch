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
import static org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequestTests.randomCommandRequest;


public class ErrorResponseTests extends ESTestCase {
    static ErrorResponse randomErrorResponse() {
        return new ErrorResponse(RequestType.COMMAND, randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomCommandRequest(), randomErrorResponse());
    }

    public void testToString() {
        assertEquals("ErrorResponse<request=[COMMAND] message=[test] cause=[test] stack=[stack\nstack]>",
                new ErrorResponse(RequestType.COMMAND, "test", "test", "stack\nstack").toString());
    }
}
