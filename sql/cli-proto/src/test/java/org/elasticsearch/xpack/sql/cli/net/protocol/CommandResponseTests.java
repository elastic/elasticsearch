/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.cli.net.protocol.CliRoundTripTestUtils.assertRoundTripCurrentVersion;
import static org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequestTests.randomCommandRequest;

public class CommandResponseTests extends ESTestCase {
    static CommandResponse randomCommandResponse() {
        long start = randomNonNegativeLong();
        long end = randomValueOtherThanMany(l -> l >= start, ESTestCase::randomNonNegativeLong);
        return new CommandResponse(start, end, randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomCommandRequest(), randomCommandResponse());
    }

    public void testToString() {
        assertEquals("CommandResponse<received=[123] sent=[332] requestId=[rid] data=[test]>",
                new CommandResponse(123, 332, "rid", "test").toString());
    }
}
