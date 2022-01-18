/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.Version;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class VersionCheckingStreamOutputTests extends ESTestCase {

    private static class DummyNamedWriteable implements NamedWriteable {

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public Version getFirstReleasedVersion() {
            return Version.V_8_1_0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public void testCheckVersionCompatibility() throws IOException {
        try (VersionCheckingStreamOutput out = new VersionCheckingStreamOutput(Version.V_8_0_0)) {
            out.writeNamedWriteable(QueryBuilders.matchAllQuery());

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> out.writeNamedWriteable(new DummyNamedWriteable())
            );
            assertEquals(
                "NamedWritable [org.elasticsearch.common.io.stream.VersionCheckingStreamOutputTests$DummyNamedWriteable] was released in "
                    + "version 8.1.0 so it cannot be serialized to a node with version 8.0.0",
                e.getMessage()
            );
        }
    }
}
