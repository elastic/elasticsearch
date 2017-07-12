/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.test.RoundTripTestUtils;

import java.io.IOException;

public final class CliRoundTripTestUtils {
    private CliRoundTripTestUtils() {
        // Just static utilities
    }

    static void assertRoundTripCurrentVersion(Request request) throws IOException {
        RoundTripTestUtils.assertRoundTrip(request, Proto.INSTANCE::writeRequest, Proto.INSTANCE::readRequest);
    }

    static void assertRoundTripCurrentVersion(Request request, Response response) throws IOException {
        RoundTripTestUtils.assertRoundTrip(response,
                (r, out) -> Proto.INSTANCE.writeResponse(r, Proto.CURRENT_VERSION, out), 
                in -> Proto.INSTANCE.readResponse(request, in));
    }
}
