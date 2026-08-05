/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class StatusInfoWireSerializingTests extends AbstractWireSerializingTestCase<StatusInfo> {

    @Override
    protected Writeable.Reader<StatusInfo> instanceReader() {
        return StatusInfo::new;
    }

    @Override
    protected StatusInfo createTestInstance() {
        return new StatusInfo(randomFrom(StatusInfo.Status.values()), randomAlphaOfLengthBetween(0, 200));
    }

    @Override
    protected StatusInfo mutateInstance(StatusInfo instance) throws IOException {
        // Since StatusInfo is a record, we don't need to check for equality
        return null;
    }

    public void testNullStatusIsRejected() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new StatusInfo(null, randomAlphaOfLengthBetween(0, 200)));
        assertEquals("Expected a non null status", e.getMessage());
    }

    public void testNullInfoIsRejected() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new StatusInfo(randomFrom(StatusInfo.Status.values()), null)
        );
        assertEquals("Expected a non null info", e.getMessage());
    }
}
