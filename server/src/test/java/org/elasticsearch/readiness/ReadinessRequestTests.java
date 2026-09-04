/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ReadinessRequestTests extends ESTestCase {

    public void testTransportRoundTrip() throws IOException {
        testRoundTrip(new ReadinessRequest(), writableRegistry(), TransportVersion.current());
    }

    private static void testRoundTrip(ReadinessRequest original, NamedWriteableRegistry namedWriteableRegistry, TransportVersion tv)
        throws IOException {
        var copy = copyWriteable(original, namedWriteableRegistry, ReadinessRequest::new, tv);
        // ReadinessRequest doesn't have equals(), but the good news is, it also has no fields.
        // We can check just the inherited parent task IDs.
        assertEquals(original.getParentTask().getId(), copy.getParentTask().getId());
    }
}
