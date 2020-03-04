/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class NodesInfoRequestTests extends ESTestCase {

    public void testNodesInfoRequestSettings() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.settings(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.settings(), equalTo(roundTrippedRequest.settings()));

        request.settings(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.settings(), equalTo(roundTrippedRequest.settings()));
    }

    public void testNodesInfoRequestOs() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.os(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.os(), equalTo(roundTrippedRequest.os()));

        request.os(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.os(), equalTo(roundTrippedRequest.os()));
    }

    public void testNodesInfoRequestProcess() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.process(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.process(), equalTo(roundTrippedRequest.process()));

        request.process(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.process(), equalTo(roundTrippedRequest.process()));
    }

    public void testNodesInfoRequestJvm() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.jvm(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.jvm(), equalTo(roundTrippedRequest.jvm()));

        request.jvm(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.jvm(), equalTo(roundTrippedRequest.jvm()));
    }

    public void testNodesInfoRequestThreadPool() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.threadPool(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.threadPool(), equalTo(roundTrippedRequest.threadPool()));

        request.threadPool(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.threadPool(), equalTo(roundTrippedRequest.threadPool()));
    }

    public void testNodesInfoRequestTransport() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.transport(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.transport(), equalTo(roundTrippedRequest.transport()));

        request.transport(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.transport(), equalTo(roundTrippedRequest.transport()));
    }

    public void testNodesInfoRequestHttp() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.http(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.http(), equalTo(roundTrippedRequest.http()));

        request.http(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.http(), equalTo(roundTrippedRequest.http()));
    }

    public void testNodesInfoRequestPlugins() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.plugins(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.plugins(), equalTo(roundTrippedRequest.plugins()));

        request.plugins(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.plugins(), equalTo(roundTrippedRequest.plugins()));
    }

    public void testNodesInfoRequestIngest() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.ingest(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.ingest(), equalTo(roundTrippedRequest.ingest()));

        request.ingest(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.ingest(), equalTo(roundTrippedRequest.ingest()));
    }

    public void testNodesInfoRequestIndices() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.indices(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.indices(), equalTo(roundTrippedRequest.indices()));

        request.indices(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.indices(), equalTo(roundTrippedRequest.indices()));
    }

    public void testNodesInfoRequestAll() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.all();
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);

        assertTrue(request.settings());
        assertTrue(roundTrippedRequest.settings());
        assertTrue(request.os());
        assertTrue(roundTrippedRequest.os());
        assertTrue(request.process());
        assertTrue(roundTrippedRequest.process());
        assertTrue(request.jvm());
        assertTrue(roundTrippedRequest.jvm());
        assertTrue(request.threadPool());
        assertTrue(roundTrippedRequest.threadPool());
        assertTrue(request.transport());
        assertTrue(roundTrippedRequest.transport());
        assertTrue(request.http());
        assertTrue(roundTrippedRequest.http());
        assertTrue(request.plugins());
        assertTrue(roundTrippedRequest.plugins());
        assertTrue(request.ingest());
        assertTrue(roundTrippedRequest.ingest());
        assertTrue(request.indices());
        assertTrue(roundTrippedRequest.indices());
    }

    public void testNodesInfoRequestClear() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.clear();
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);

        assertFalse(request.settings());
        assertFalse(roundTrippedRequest.settings());
        assertFalse(request.os());
        assertFalse(roundTrippedRequest.os());
        assertFalse(request.process());
        assertFalse(roundTrippedRequest.process());
        assertFalse(request.jvm());
        assertFalse(roundTrippedRequest.jvm());
        assertFalse(request.threadPool());
        assertFalse(roundTrippedRequest.threadPool());
        assertFalse(request.transport());
        assertFalse(roundTrippedRequest.transport());
        assertFalse(request.http());
        assertFalse(roundTrippedRequest.http());
        assertFalse(request.plugins());
        assertFalse(roundTrippedRequest.plugins());
        assertFalse(request.ingest());
        assertFalse(roundTrippedRequest.ingest());
        assertFalse(request.indices());
        assertFalse(roundTrippedRequest.indices());
    }

    private NodesInfoRequest roundTripRequest(NodesInfoRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesInfoRequest(in);
            }
        }
    }
}
