package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class NodesInfoRequestTest {

    @Test
    public void testNodesInfoRequestSettings() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.settings(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.settings(), equalTo(roundTrippedRequest.settings()));

        request.settings(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.settings(), equalTo(roundTrippedRequest.settings()));
    }

    @Test
    public void testNodesInfoRequestOs() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.os(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.os(), equalTo(roundTrippedRequest.os()));

        request.os(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.os(), equalTo(roundTrippedRequest.os()));
    }

    @Test
    public void testNodesInfoRequestProcess() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.process(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.process(), equalTo(roundTrippedRequest.process()));

        request.process(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.process(), equalTo(roundTrippedRequest.process()));
    }

    @Test
    public void testNodesInfoRequestJvm() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.jvm(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.jvm(), equalTo(roundTrippedRequest.jvm()));

        request.jvm(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.jvm(), equalTo(roundTrippedRequest.jvm()));
    }

    @Test
    public void testNodesInfoRequestThreadPool() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.threadPool(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.threadPool(), equalTo(roundTrippedRequest.threadPool()));

        request.threadPool(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.threadPool(), equalTo(roundTrippedRequest.threadPool()));
    }

    @Test
    public void testNodesInfoRequestTransport() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.transport(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.transport(), equalTo(roundTrippedRequest.transport()));

        request.transport(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.transport(), equalTo(roundTrippedRequest.transport()));
    }

    @Test
    public void testNodesInfoRequestHttp() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.http(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.http(), equalTo(roundTrippedRequest.http()));

        request.http(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.http(), equalTo(roundTrippedRequest.http()));
    }

    @Test
    public void testNodesInfoRequestPlugins() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.plugins(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.plugins(), equalTo(roundTrippedRequest.plugins()));

        request.plugins(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.plugins(), equalTo(roundTrippedRequest.plugins()));
    }

    @Test
    public void testNodesInfoRequestIngest() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.ingest(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.ingest(), equalTo(roundTrippedRequest.ingest()));

        request.ingest(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.ingest(), equalTo(roundTrippedRequest.ingest()));
    }

    @Test
    public void testNodesInfoRequestIndices() throws Exception {
        NodesInfoRequest request = new NodesInfoRequest("node");
        request.indices(true);
        NodesInfoRequest roundTrippedRequest = roundTripRequest(request);
        assertThat(request.indices(), equalTo(roundTrippedRequest.indices()));

        request.indices(false);
        roundTrippedRequest = roundTripRequest(request);
        assertThat(request.indices(), equalTo(roundTrippedRequest.indices()));
    }

    @Test
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

    @Test
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
