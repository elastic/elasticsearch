/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.jdbc.net.protocol.TimeoutInfoTests.randomTimeoutInfo;
import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;

public class QueryInitRequestTests extends ESTestCase {
    public static QueryInitRequest randomQueryInitRequest() {
        return new QueryInitRequest(between(0, Integer.MAX_VALUE), randomAlphaOfLength(5), randomTimeZone(random()), randomTimeoutInfo());
    }

    public void testRoundTrip() throws IOException {
        assertRoundTrip(randomQueryInitRequest(), QueryInitRequest::encode, in -> (QueryInitRequest) ProtoUtils.readRequest(in));
    }
}
