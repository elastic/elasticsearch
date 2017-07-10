/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.cli.net.protocol.CliRoundTripTestUtils.assertRoundTripCurrentVersion;

public class InfoResponseTests extends ESTestCase {
    static InfoResponse randomInfoResponse() {
        return new InfoResponse(randomAlphaOfLength(5), randomAlphaOfLength(5), randomByte(), randomByte(),
                randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    public void testRoundTrip() throws IOException {
        assertRoundTripCurrentVersion(randomInfoResponse());
    }
}
