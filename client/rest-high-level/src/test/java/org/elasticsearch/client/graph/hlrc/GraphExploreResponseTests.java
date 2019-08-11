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
package org.elasticsearch.client.graph.hlrc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.graph.Connection;
import org.elasticsearch.client.graph.GraphExploreResponse;
import org.elasticsearch.client.graph.Vertex;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.protocol.xpack.graph.Connection.ConnectionId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class GraphExploreResponseTests extends AbstractHlrcXContentTestCase<
    org.elasticsearch.protocol.xpack.graph.GraphExploreResponse,
    GraphExploreResponse> {

    static final Function<Vertex.VertexId, org.elasticsearch.protocol.xpack.graph.Vertex.VertexId> VERTEX_ID_FUNCTION =
        vId -> new org.elasticsearch.protocol.xpack.graph.Vertex.VertexId(vId.getField(), vId.getTerm());
    static final Function<Vertex, org.elasticsearch.protocol.xpack.graph.Vertex> VERTEX_FUNCTION =
        v -> new org.elasticsearch.protocol.xpack.graph.Vertex(v.getField(), v.getTerm(), v.getWeight(),
            v.getHopDepth(), v.getBg(), v.getFg());

    @Override
    public GraphExploreResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return GraphExploreResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.protocol.xpack.graph.GraphExploreResponse convertHlrcToInternal(GraphExploreResponse instance) {
        return new org.elasticsearch.protocol.xpack.graph.GraphExploreResponse(instance.getTookInMillis(), instance.isTimedOut(),
            instance.getShardFailures(), convertVertices(instance), convertConnections(instance), instance.isReturnDetailedInfo());
    }

    public Map<org.elasticsearch.protocol.xpack.graph.Vertex.VertexId, org.elasticsearch.protocol.xpack.graph.Vertex> convertVertices(
        GraphExploreResponse instance) {
        final Collection<Vertex.VertexId> vertexIds = instance.getVertexIds();
        final Map<org.elasticsearch.protocol.xpack.graph.Vertex.VertexId, org.elasticsearch.protocol.xpack.graph.Vertex> vertexMap =
            new LinkedHashMap<>(vertexIds.size());

        for (Vertex.VertexId vertexId : vertexIds) {
            final Vertex vertex = instance.getVertex(vertexId);

            vertexMap.put(VERTEX_ID_FUNCTION.apply(vertexId), VERTEX_FUNCTION.apply(vertex));
        }
        return vertexMap;
    }

    public Map<ConnectionId, org.elasticsearch.protocol.xpack.graph.Connection> convertConnections(GraphExploreResponse instance) {
        final Collection<Connection.ConnectionId> connectionIds = instance.getConnectionIds();
        final Map<ConnectionId,org.elasticsearch.protocol.xpack.graph.Connection> connectionMap= new LinkedHashMap<>(connectionIds.size());
        for (Connection.ConnectionId connectionId : connectionIds) {
            final Connection connection = instance.getConnection(connectionId);
            final ConnectionId connectionId1 = new ConnectionId(VERTEX_ID_FUNCTION.apply(connectionId.getSource()),
                    VERTEX_ID_FUNCTION.apply(connectionId.getTarget()));
            final org.elasticsearch.protocol.xpack.graph.Connection connection1 = new org.elasticsearch.protocol.xpack.graph.Connection(
                VERTEX_FUNCTION.apply(connection.getFrom()), VERTEX_FUNCTION.apply(connection.getTo()), connection.getWeight(),
                connection.getDocCount());
            connectionMap.put(connectionId1, connection1);
        }
        return connectionMap;
    }

    @Override
    protected org.elasticsearch.protocol.xpack.graph.GraphExploreResponse createTestInstance() {
        return createInstance(0);
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
    protected org.elasticsearch.protocol.xpack.graph.GraphExploreResponse doParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.protocol.xpack.graph.GraphExploreResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
    
    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[]{"vertices", "connections"};
    }    

    protected Predicate<String> getRandomFieldsExcludeFilterWhenResultHasErrors() {
        return field -> field.startsWith("responses");
    }    

    @Override
    protected void assertEqualInstances(org.elasticsearch.protocol.xpack.graph.GraphExploreResponse expectedInstance,
                                        org.elasticsearch.protocol.xpack.graph.GraphExploreResponse newInstance) {
        Assert.assertThat(newInstance.getTook(), equalTo(expectedInstance.getTook()));
        Assert.assertThat(newInstance.isTimedOut(), equalTo(expectedInstance.isTimedOut()));
        
        Comparator<org.elasticsearch.protocol.xpack.graph.Connection> connComparator =
            Comparator.comparing(o -> o.getId().toString());
        org.elasticsearch.protocol.xpack.graph.Connection[] newConns =
            newInstance.getConnections().toArray(new org.elasticsearch.protocol.xpack.graph.Connection[0]);
        org.elasticsearch.protocol.xpack.graph.Connection[] expectedConns =
            expectedInstance.getConnections().toArray(new org.elasticsearch.protocol.xpack.graph.Connection[0]);
        Arrays.sort(newConns, connComparator);
        Arrays.sort(expectedConns, connComparator);
        Assert.assertArrayEquals(expectedConns, newConns);
        
        //Sort the vertices lists before equality test (map insertion sequences can cause order differences)
        Comparator<org.elasticsearch.protocol.xpack.graph.Vertex> comparator = Comparator.comparing(o -> o.getId().toString());
        org.elasticsearch.protocol.xpack.graph.Vertex[] newVertices =
            newInstance.getVertices().toArray(new org.elasticsearch.protocol.xpack.graph.Vertex[0]);
        org.elasticsearch.protocol.xpack.graph.Vertex[] expectedVertices =
            expectedInstance.getVertices().toArray(new org.elasticsearch.protocol.xpack.graph.Vertex[0]);
        Arrays.sort(newVertices, comparator);
        Arrays.sort(expectedVertices, comparator);
        Assert.assertArrayEquals(expectedVertices, newVertices);
        
        ShardOperationFailedException[] newFailures = newInstance.getShardFailures();
        ShardOperationFailedException[] expectedFailures = expectedInstance.getShardFailures();
        Assert.assertEquals(expectedFailures.length, newFailures.length);
        
    }
    
    /**
     * Test parsing {@link  org.elasticsearch.protocol.xpack.graph.GraphExploreResponse} with inner failures as they
     * don't support asserting on xcontent equivalence, given exceptions are not parsed back as the same original class.
     * We run the usual {@link AbstractXContentTestCase#testFromXContent()} without failures, and this other test with
     * failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<org.elasticsearch.protocol.xpack.graph.GraphExploreResponse> instanceSupplier =
            GraphExploreResponseTests::createTestInstanceWithFailures;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
                AbstractXContentTestCase.NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields, getShuffleFieldsExceptions(),
                getRandomFieldsExcludeFilterWhenResultHasErrors(), this::createParser, this::doParseInstance,
                this::assertEqualInstances, assertToXContentEquivalence, ToXContent.EMPTY_PARAMS);
    }    

}
