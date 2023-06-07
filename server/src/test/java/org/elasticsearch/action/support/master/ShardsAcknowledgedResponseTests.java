/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class ShardsAcknowledgedResponseTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ShardsAcknowledgedResponse testInstance = new TestImpl(true, true);

        ShardsAcknowledgedResponse result = copyWriteable(
            testInstance,
            new NamedWriteableRegistry(List.of()),
            in -> new TestImpl(in, true),
                TransportVersion.current()
        );
        assertThat(result.isAcknowledged(), is(true));
        assertThat(result.isShardsAcknowledged(), is(true));

        result = copyWriteable(
            testInstance,
            new NamedWriteableRegistry(List.of()),
            in -> new TestImpl(in, false),
                TransportVersion.current()
        );
        assertThat(result.isAcknowledged(), is(true));
        assertThat(result.isShardsAcknowledged(), is(false));
    }

    private static class TestImpl extends ShardsAcknowledgedResponse {

        private TestImpl(StreamInput in, boolean readShardsAcknowledged) throws IOException {
            super(in, readShardsAcknowledged);
        }

        private TestImpl(boolean acknowledged, boolean shardsAcknowledged) {
            super(acknowledged, shardsAcknowledged);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeShardsAcknowledged(out);
        }
    }

}
