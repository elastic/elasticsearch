/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.test.AbstractXContentTestCase;

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

public class GraphExploreResponseTests extends
        AbstractHlrcXContentTestCase<GraphExploreResponse, org.elasticsearch.client.graph.GraphExploreResponse> {

    static final Function<org.elasticsearch.client.graph.Vertex.VertexId, Vertex.VertexId> VERTEX_ID_FUNCTION =
        vId -> new Vertex.VertexId(vId.getField(), vId.getTerm());
    static final Function<org.elasticsearch.client.graph.Vertex, Vertex> VERTEX_FUNCTION =
        v -> new Vertex(v.getField(), v.getTerm(), v.getWeight(), v.getHopDepth(), v.getBg(), v.getFg());

    @Override
    public org.elasticsearch.client.graph.GraphExploreResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.graph.GraphExploreResponse.fromXContent(parser);
    }

    @Override
    public GraphExploreResponse convertHlrcToInternal(org.elasticsearch.client.graph.GraphExploreResponse instance) {
        return new GraphExploreResponse(instance.getTookInMillis(), instance.isTimedOut(),
            instance.getShardFailures(), convertVertices(instance), convertConnections(instance), instance.isReturnDetailedInfo());
    }

    public Map<Vertex.VertexId, Vertex> convertVertices(org.elasticsearch.client.graph.GraphExploreResponse instance) {
        final Collection<org.elasticsearch.client.graph.Vertex.VertexId> vertexIds = instance.getVertexIds();
        final Map<Vertex.VertexId, Vertex> vertexMap = new LinkedHashMap<>(vertexIds.size());

        for (org.elasticsearch.client.graph.Vertex.VertexId vertexId : vertexIds) {
            final org.elasticsearch.client.graph.Vertex vertex = instance.getVertex(vertexId);

            vertexMap.put(VERTEX_ID_FUNCTION.apply(vertexId), VERTEX_FUNCTION.apply(vertex));
        }
        return vertexMap;
    }

    public Map<Connection.ConnectionId, Connection> convertConnections(org.elasticsearch.client.graph.GraphExploreResponse instance) {
        final Collection<org.elasticsearch.client.graph.Connection.ConnectionId> connectionIds = instance.getConnectionIds();
        final Map<Connection.ConnectionId, Connection> connectionMap = new LinkedHashMap<>(connectionIds.size());
        for (org.elasticsearch.client.graph.Connection.ConnectionId connectionId : connectionIds) {
            final org.elasticsearch.client.graph.Connection connection = instance.getConnection(connectionId);
            final Connection.ConnectionId connectionId1 =
                new Connection.ConnectionId(VERTEX_ID_FUNCTION.apply(connectionId.getSource()),
                    VERTEX_ID_FUNCTION.apply(connectionId.getTarget()));
            final Connection connection1 = new Connection(VERTEX_FUNCTION.apply(connection.getFrom()),
                VERTEX_FUNCTION.apply(connection.getTo()),
                connection.getWeight(), connection.getDocCount());
            connectionMap.put(connectionId1, connection1);
        }
        return connectionMap;
    }

    @Override
    protected  GraphExploreResponse createTestInstance() {
        return createInstance(0);
    }

    private static  GraphExploreResponse createInstance(int numFailures) {
        int numItems = randomIntBetween(4, 128);
        boolean timedOut = randomBoolean();
        boolean showDetails = randomBoolean();
        long overallTookInMillis = randomNonNegativeLong();
        Map<Vertex.VertexId, Vertex> vertices = new HashMap<>();
        Map<Connection.ConnectionId, Connection> connections = new HashMap<>();
        ShardOperationFailedException [] failures = new ShardOperationFailedException [numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = new ShardSearchFailure(new ElasticsearchException("an error"));
        }
        
        //Create random set of vertices
        for (int i = 0; i < numItems; i++) {
            Vertex v = new Vertex("field1", randomAlphaOfLength(5), randomDouble(), 0, 
                    showDetails?randomIntBetween(100, 200):0, 
                    showDetails?randomIntBetween(1, 100):0);
            vertices.put(v.getId(), v);
        }
        
        //Wire up half the vertices randomly
        Vertex[] vs = vertices.values().toArray(new Vertex[vertices.size()]);
        for (int i = 0; i < numItems/2; i++) {
            Vertex v1 = vs[randomIntBetween(0, vs.length-1)];
            Vertex v2 = vs[randomIntBetween(0, vs.length-1)];
            if(v1 != v2) {
                Connection conn = new Connection(v1, v2, randomDouble(), randomLongBetween(1, 10));
                connections.put(conn.getId(), conn);
            }
        }
        return new  GraphExploreResponse(overallTookInMillis, timedOut, failures, vertices, connections, showDetails);
    }
    

    private static GraphExploreResponse createTestInstanceWithFailures() {
        return createInstance(randomIntBetween(1, 128));
    }

    @Override
    protected  GraphExploreResponse doParseInstance(XContentParser parser) throws IOException {
        return GraphExploreResponse.fromXContent(parser);
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
    protected void assertEqualInstances( GraphExploreResponse expectedInstance,  GraphExploreResponse newInstance) {
        assertThat(newInstance.getTook(), equalTo(expectedInstance.getTook()));
        assertThat(newInstance.isTimedOut(), equalTo(expectedInstance.isTimedOut()));
        
        Comparator<Connection> connComparator = new Comparator<Connection>() {
            @Override
            public int compare(Connection o1, Connection o2) {
                return o1.getId().toString().compareTo(o2.getId().toString());
            }
        };
        Connection[] newConns = newInstance.getConnections().toArray(new Connection[0]);
        Connection[] expectedConns = expectedInstance.getConnections().toArray(new Connection[0]);
        Arrays.sort(newConns, connComparator);
        Arrays.sort(expectedConns, connComparator);
        assertArrayEquals(expectedConns, newConns);
        
        //Sort the vertices lists before equality test (map insertion sequences can cause order differences)
        Comparator<Vertex> comparator = new Comparator<Vertex>() {
            @Override
            public int compare(Vertex o1, Vertex o2) {
                return o1.getId().toString().compareTo(o2.getId().toString());
            }
        };
        Vertex[] newVertices = newInstance.getVertices().toArray(new Vertex[0]);        
        Vertex[] expectedVertices = expectedInstance.getVertices().toArray(new Vertex[0]);
        Arrays.sort(newVertices, comparator);
        Arrays.sort(expectedVertices, comparator);
        assertArrayEquals(expectedVertices, newVertices);
        
        ShardOperationFailedException[] newFailures = newInstance.getShardFailures();
        ShardOperationFailedException[] expectedFailures = expectedInstance.getShardFailures();
        assertEquals(expectedFailures.length, newFailures.length);
        
    }
    
    /**
     * Test parsing {@link  GraphExploreResponse} with inner failures as they don't support asserting on xcontent equivalence, given
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier< GraphExploreResponse> instanceSupplier = GraphExploreResponseTests::createTestInstanceWithFailures;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
                NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields, getShuffleFieldsExceptions(),
                getRandomFieldsExcludeFilterWhenResultHasErrors(), this::createParser, this::doParseInstance,
                this::assertEqualInstances, assertToXContentEquivalence, ToXContent.EMPTY_PARAMS);
    }    

}
