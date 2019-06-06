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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class GraphExploreResponseTests extends AbstractXContentTestCase<GraphExploreResponse> {

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
    

    private static   GraphExploreResponse createTestInstanceWithFailures() {
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
