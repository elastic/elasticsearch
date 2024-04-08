/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class VersionCheckingStreamOutputTests extends ESTestCase {

    private static class DummyNamedWriteable implements VersionedNamedWriteable {

        @Override
        public String getWriteableName() {
            return "test_writable";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    }

    public void testCheckVersionCompatibility() throws IOException {
        TransportVersion streamVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.MINIMUM_COMPATIBLE,
            TransportVersionUtils.getPreviousVersion(TransportVersion.current())
        );
        try (VersionCheckingStreamOutput out = new VersionCheckingStreamOutput(streamVersion)) {
            out.writeNamedWriteable(QueryBuilders.matchAllQuery());

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> out.writeNamedWriteable(new DummyNamedWriteable())
            );
            assertEquals(
                "[test_writable] was released first in version "
                    + TransportVersion.current()
                    + ", failed compatibility check trying to send it to node with version "
                    + streamVersion,
                e.getMessage()
            );
        }
    }
}
