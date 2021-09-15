/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.hotthreads;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NodesHotThreadsRequestTests extends ESTestCase {

    /** Simple override of BaseNodesRequest to ensure we read the
     * common fields of the nodes request.
     */
    static class NodesHotThreadsRequestHelper extends BaseNodesRequest<NodesHotThreadsRequestHelper> {
        NodesHotThreadsRequestHelper(StreamInput in) throws IOException {
            super(in);
        }

        NodesHotThreadsRequestHelper(String... nodesIds) {
            super(nodesIds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public void testBWCSerialization() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();

        TimeValue sampleInterval = new TimeValue(50, TimeUnit.MINUTES);

        NodesHotThreadsRequestHelper outHelper = new NodesHotThreadsRequestHelper("123");

        outHelper.writeTo(out);
        // Write manually some values that differ from the defaults
        // in NodesHotThreadsRequest
        out.writeInt(4); // threads
        out.writeBoolean(false); // ignoreIdleThreads
        out.writeString("block"); // type
        out.writeTimeValue(sampleInterval); // interval
        out.writeInt(3); // snapshots

        NodesHotThreadsRequest inRequest = new NodesHotThreadsRequest(out.bytes().streamInput());

        assertEquals(4, inRequest.threads());
        assertFalse(inRequest.ignoreIdleThreads());
        assertEquals(HotThreads.ReportType.BLOCK, inRequest.type());
        assertEquals(sampleInterval, inRequest.interval());
        assertEquals(3, inRequest.snapshots());

        // Change the report type enum
        inRequest.type(HotThreads.ReportType.WAIT);

        BytesStreamOutput writeOut = new BytesStreamOutput();
        inRequest.writeTo(writeOut);

        StreamInput whatWeWrote = writeOut.bytes().streamInput();

        // We construct the helper to read the common serialized fields from the in.
        new NodesHotThreadsRequestHelper(whatWeWrote);
        // Make sure we serialized in the following format
        assertEquals(4, whatWeWrote.readInt());
        assertFalse(whatWeWrote.readBoolean());
        assertEquals("wait", whatWeWrote.readString()); // lowercase enum value, not label
        assertEquals(sampleInterval, whatWeWrote.readTimeValue());
        assertEquals(3, whatWeWrote.readInt());
    }
}
