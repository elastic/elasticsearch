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
package org.elasticsearch.client.graph;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.graph.Connection.ConnectionId;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GraphExploreResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.protocol.xpack.graph.GraphExploreResponse, GraphExploreResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.graph.GraphExploreResponse createServerTestInstance(XContentType xContentType) {
        return createInstance(randomIntBetween(1, 128));
    }

    private static org.elasticsearch.protocol.xpack.graph.GraphExploreResponse createInstance(int numFailures) {
        int numItems = randomIntBetween(4, 128);
        boolean timedOut = randomBoolean();
        boolean showDetails = randomBoolean();
        long overallTookInMillis = randomNonNegativeLong();
        Map<org.elasticsearch.protocol.xpack.graph.Vertex.VertexId, org.elasticsearch.protocol.xpack.graph.Vertex> vertices =
            new HashMap<>();
        Map<ConnectionId,
            org.elasticsearch.protocol.xpack.graph.Connection> connections = new HashMap<>();
        ShardOperationFailedException [] failures = new ShardOperationFailedException [numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = new ShardSearchFailure(new ElasticsearchException("an error"));
        }

        //Create random set of vertices
        for (int i = 0; i < numItems; i++) {
            org.elasticsearch.protocol.xpack.graph.Vertex v = new org.elasticsearch.protocol.xpack.graph.Vertex("field1",
                randomAlphaOfLength(5), randomDouble(), 0,
                    showDetails? randomIntBetween(100, 200):0,
                    showDetails? randomIntBetween(1, 100):0);
            vertices.put(v.getId(), v);
        }

        //Wire up half the vertices randomly
        org.elasticsearch.protocol.xpack.graph.Vertex[] vs =
            vertices.values().toArray(new org.elasticsearch.protocol.xpack.graph.Vertex[vertices.size()]);
        for (int i = 0; i < numItems/2; i++) {
            org.elasticsearch.protocol.xpack.graph.Vertex v1 = vs[randomIntBetween(0, vs.length-1)];
            org.elasticsearch.protocol.xpack.graph.Vertex v2 = vs[randomIntBetween(0, vs.length-1)];
            if(v1 != v2) {
                org.elasticsearch.protocol.xpack.graph.Connection conn = new org.elasticsearch.protocol.xpack.graph.Connection(v1, v2,
                    randomDouble(), randomLongBetween(1, 10));
                connections.put(conn.getId(), conn);
            }
        }
        return new org.elasticsearch.protocol.xpack.graph.GraphExploreResponse(overallTookInMillis, timedOut, failures,
            vertices, connections, showDetails);
    }


    private static org.elasticsearch.protocol.xpack.graph.GraphExploreResponse createTestInstanceWithFailures() {
        return createInstance(randomIntBetween(1, 128));
    }

    @Override
    protected void assertInstances(org.elasticsearch.protocol.xpack.graph.GraphExploreResponse serverTestInstance,
                                   GraphExploreResponse clientInstance) {
        Assert.assertThat(serverTestInstance.getTook(), equalTo(clientInstance.getTook()));
        Assert.assertThat(serverTestInstance.isTimedOut(), equalTo(clientInstance.isTimedOut()));

        Comparator<org.elasticsearch.protocol.xpack.graph.Connection> serverComparator =
            Comparator.comparing(o -> o.getId().toString());
        org.elasticsearch.protocol.xpack.graph.Connection[] serverConns =
            serverTestInstance.getConnections().toArray(new org.elasticsearch.protocol.xpack.graph.Connection[0]);
        Comparator<Connection> clientComparator =
            Comparator.comparing(o -> o.getId().toString());
        Connection[] clientConns =
            clientInstance.getConnections().toArray(new Connection[0]);
        Arrays.sort(serverConns, serverComparator);
        Arrays.sort(clientConns, clientComparator);
        assertThat(serverConns.length, equalTo(clientConns.length));
        for (int i = 0; i < clientConns.length ; i++) {
            org.elasticsearch.protocol.xpack.graph.Connection serverConn = serverConns[i];
            Connection clientConn = clientConns[i];
            // String rep since they are different classes
            assertThat(serverConn.getId().toString(), equalTo(clientConn.getId().toString()));
            assertVertex(serverConn.getTo(), clientConn.getTo());
            assertThat(serverConn.getDocCount(), equalTo(clientConn.getDocCount()));
            assertVertex(serverConn.getFrom(), clientConn.getFrom());
            assertThat(serverConn.getWeight(), equalTo(clientConn.getWeight()));
        }

        //Sort the vertices lists before equality test (map insertion sequences can cause order differences)
        Comparator<org.elasticsearch.protocol.xpack.graph.Vertex> serverVertexComparator = Comparator.comparing(o -> o.getId().toString());
        org.elasticsearch.protocol.xpack.graph.Vertex[] serverVertices =
            serverTestInstance.getVertices().toArray(new org.elasticsearch.protocol.xpack.graph.Vertex[0]);
        Comparator<Vertex> clientVertexComparator = Comparator.comparing(o -> o.getId().toString());
        Vertex[] clientVerticies = clientInstance.getVertices().toArray(new Vertex[0]);
        Arrays.sort(serverVertices, serverVertexComparator);
        Arrays.sort(clientVerticies, clientVertexComparator);
        assertThat(serverVertices.length, equalTo(clientVerticies.length));
        for (int i = 0; i < serverVertices.length; i++) {
            org.elasticsearch.protocol.xpack.graph.Vertex serverVertex = serverVertices[i];
            Vertex clientVertex = clientVerticies[i];
            assertVertex(serverVertex, clientVertex);
        }

        ShardOperationFailedException[] newFailures = serverTestInstance.getShardFailures();
        ShardOperationFailedException[] expectedFailures = clientInstance.getShardFailures();
        Assert.assertEquals(expectedFailures.length, newFailures.length);
    }

    private void assertVertex(org.elasticsearch.protocol.xpack.graph.Vertex server, Vertex client) {
        assertThat(client.getId().toString(), equalTo(server.getId().toString()));
        assertThat(client.getTerm(), equalTo(server.getTerm()));
        assertThat(client.getField(), equalTo(server.getField()));
        assertThat(client.getHopDepth(), equalTo(server.getHopDepth()));
        assertThat(client.getFg(), equalTo(server.getFg()));
        assertThat(client.getBg(), equalTo(server.getBg()));
        assertThat(client.getWeight(), equalTo(server.getWeight()));
    }

    @Override
    protected GraphExploreResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GraphExploreResponse.fromXContent(parser);
    }
}
