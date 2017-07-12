/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;

public class TimeoutInfoTests extends ESTestCase {
    static TimeoutInfo randomTimeoutInfo() {
        return new TimeoutInfo(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    public void testRoundTrip() throws IOException {
        assertRoundTrip(randomTimeoutInfo(), TimeoutInfo::write, TimeoutInfo::new);
    }
}
