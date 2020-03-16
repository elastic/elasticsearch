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

/**
 * Granular tests for the {@link NodesInfoRequest} class. Higher-level tests
 * can be found in {@link org.elasticsearch.rest.action.admin.cluster.RestNodesInfoActionTests}.
 */
public class NodesInfoRequestTests extends ESTestCase {

    /**
     * Make sure that we can set, serialize, and deserialize arbitrary sets
     * of metrics.
     *
     * TODO: Once we can set values by string, use a collection rather than
     *   checking each and every setter in the public API
     */
    public void testMetricsSetters() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest(randomAlphaOfLength(8));
        request.settings(randomBoolean());
        request.os(randomBoolean());
        request.process(randomBoolean());
        request.jvm(randomBoolean());
        request.threadPool(randomBoolean());
        request.transport(randomBoolean());
        request.http(randomBoolean());
        request.plugins(randomBoolean());
        request.ingest(randomBoolean());
        request.indices(randomBoolean());
        NodesInfoRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Test that a newly constructed NodesInfoRequestObject requests all of the
     * possible metrics defined in {@link NodesInfoRequest.Metrics}.
     */
    public void testNodesInfoRequestDefaults() {
        NodesInfoRequest defaultNodesInfoRequest = new NodesInfoRequest(randomAlphaOfLength(8));
        NodesInfoRequest allMetricsNodesInfoRequest = new NodesInfoRequest(randomAlphaOfLength(8));
        allMetricsNodesInfoRequest.all();

        assertRequestsEqual(defaultNodesInfoRequest, allMetricsNodesInfoRequest);
    }

    /**
     * Test that the {@link NodesInfoRequest#all()} method sets all of the
     * metrics to {@code true}.
     *
     * TODO: Once we can check values by string, use a collection rather than
     *   checking each and every getter in the public API
     */
    public void testNodesInfoRequestAll() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.all();

        assertTrue(request.settings());
        assertTrue(request.os());
        assertTrue(request.process());
        assertTrue(request.jvm());
        assertTrue(request.threadPool());
        assertTrue(request.transport());
        assertTrue(request.http());
        assertTrue(request.plugins());
        assertTrue(request.ingest());
        assertTrue(request.indices());
    }

    /**
     * Test that the {@link NodesInfoRequest#clear()} method sets all of the
     * metrics to {@code false}.
     *
     * TODO: Once we can check values by string, use a collection rather than
     *   checking each and every getter in the public API
     */
    public void testNodesInfoRequestClear() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.clear();

        assertFalse(request.settings());
        assertFalse(request.os());
        assertFalse(request.process());
        assertFalse(request.jvm());
        assertFalse(request.threadPool());
        assertFalse(request.transport());
        assertFalse(request.http());
        assertFalse(request.plugins());
        assertFalse(request.ingest());
        assertFalse(request.indices());
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static NodesInfoRequest roundTripRequest(NodesInfoRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new NodesInfoRequest(in);
            }
        }
    }

    private static void assertRequestsEqual(NodesInfoRequest request1, NodesInfoRequest request2) {

        // TODO: Once we can check values by string, use a collection rather than
        //   checking each and every getter in the public API
        assertThat(request1.settings(), equalTo(request2.settings()));
        assertThat(request1.os(), equalTo(request2.os()));
        assertThat(request1.process(), equalTo(request2.process()));
        assertThat(request1.jvm(), equalTo(request2.jvm()));
        assertThat(request1.threadPool(), equalTo(request2.threadPool()));
        assertThat(request1.transport(), equalTo(request2.transport()));
        assertThat(request1.http(), equalTo(request2.http()));
        assertThat(request1.plugins(), equalTo(request2.plugins()));
        assertThat(request1.ingest(), equalTo(request2.ingest()));
        assertThat(request1.indices(), equalTo(request2.indices()));
    }
}
