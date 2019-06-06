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

import com.carrotsearch.hppc.ObjectIntHashMap;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.graph.Connection.ConnectionId;
import org.elasticsearch.client.graph.Connection.UnresolvedConnection;
import org.elasticsearch.client.graph.Vertex.VertexId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Graph explore response holds a graph of {@link Vertex} and {@link Connection} objects
 * (nodes and edges in common graph parlance).
 * 
 * @see GraphExploreRequest
 */
public class GraphExploreResponse implements ToXContentObject {

    private long tookInMillis;
    private boolean timedOut = false;
    private ShardOperationFailedException[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;
    private Map<VertexId, Vertex> vertices;
    private Map<ConnectionId, Connection> connections;
    private boolean returnDetailedInfo;
    static final String RETURN_DETAILED_INFO_PARAM = "returnDetailedInfo";

    public GraphExploreResponse() {
    }

    public GraphExploreResponse(long tookInMillis, boolean timedOut, ShardOperationFailedException[] shardFailures,
                                Map<VertexId, Vertex> vertices, Map<ConnectionId, Connection> connections, boolean returnDetailedInfo) {
        this.tookInMillis = tookInMillis;
        this.timedOut = timedOut;
        this.shardFailures = shardFailures;
        this.vertices = vertices;
        this.connections = connections;
        this.returnDetailedInfo = returnDetailedInfo;
    }


    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * @return true if the time stated in {@link GraphExploreRequest#timeout(TimeValue)} was exceeded
     * (not all hops may have been completed in this case)
     */
    public boolean isTimedOut() {
        return this.timedOut;
    }
    public ShardOperationFailedException[] getShardFailures() {
        return shardFailures;
    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Collection<ConnectionId> getConnectionIds() {
        return connections.keySet();
    }

    public Connection getConnection(ConnectionId connectionId) {
        return connections.get(connectionId);
    }

    public Collection<Vertex> getVertices() {
        return vertices.values();
    }

    public Collection<VertexId> getVertexIds() {
        return vertices.keySet();
    }

    public Vertex getVertex(VertexId id) {
        return vertices.get(id);
    }

    public boolean isReturnDetailedInfo() {
        return returnDetailedInfo;
    }

    private static final ParseField TOOK = new ParseField("took");
    private static final ParseField TIMED_OUT = new ParseField("timed_out");
    private static final ParseField VERTICES = new ParseField("vertices");
    private static final ParseField CONNECTIONS = new ParseField("connections");
    private static final ParseField FAILURES = new ParseField("failures");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOOK.getPreferredName(), tookInMillis);
        builder.field(TIMED_OUT.getPreferredName(), timedOut);

        builder.startArray(FAILURES.getPreferredName());
        if (shardFailures != null) {
            for (ShardOperationFailedException shardFailure : shardFailures) {
                shardFailure.toXContent(builder, params);
            }
        }
        builder.endArray();

        ObjectIntHashMap<Vertex> vertexNumbers = new ObjectIntHashMap<>(vertices.size());
        
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put(RETURN_DETAILED_INFO_PARAM, Boolean.toString(returnDetailedInfo));
        Params extendedParams = new DelegatingMapParams(extraParams, params);
        
        builder.startArray(VERTICES.getPreferredName());
        for (Vertex vertex : vertices.values()) {
            builder.startObject();
            vertexNumbers.put(vertex, vertexNumbers.size());
            vertex.toXContent(builder, extendedParams);            
            builder.endObject();
        }
        builder.endArray();

        builder.startArray(CONNECTIONS.getPreferredName());
        for (Connection connection : connections.values()) {
            builder.startObject();
            connection.toXContent(builder, extendedParams, vertexNumbers);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<GraphExploreResponse, Void> PARSER = new ConstructingObjectParser<>(
            "GraphExploreResponsenParser", true,
            args -> {
                GraphExploreResponse result = new GraphExploreResponse();  
                result.vertices = new HashMap<>();
                result.connections = new HashMap<>();
                
                result.tookInMillis = (Long) args[0];
                result.timedOut = (Boolean) args[1];
                
                @SuppressWarnings("unchecked")
                List<Vertex> vertices = (List<Vertex>) args[2];
                @SuppressWarnings("unchecked")
                List<UnresolvedConnection> unresolvedConnections = (List<UnresolvedConnection>) args[3];
                @SuppressWarnings("unchecked")
                List<ShardSearchFailure> failures = (List<ShardSearchFailure>) args[4];
                for (Vertex vertex : vertices) {
                    // reverse-engineer if detailed stats were requested -
                    // mainly here for testing framework's equality tests
                    result.returnDetailedInfo = result.returnDetailedInfo || vertex.getFg() > 0;
                    result.vertices.put(vertex.getId(), vertex);
                }
                for (UnresolvedConnection unresolvedConnection : unresolvedConnections) {
                    Connection resolvedConnection = unresolvedConnection.resolve(vertices);
                    result.connections.put(resolvedConnection.getId(), resolvedConnection);
                }
                if (failures.size() > 0) {
                    result.shardFailures = failures.toArray(new ShardSearchFailure[failures.size()]);
                }      
                return result;
            });

    static {
        PARSER.declareLong(constructorArg(), TOOK);
        PARSER.declareBoolean(constructorArg(), TIMED_OUT);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Vertex.fromXContent(p), VERTICES);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> UnresolvedConnection.fromXContent(p), CONNECTIONS);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ShardSearchFailure.fromXContent(p), FAILURES);
    } 
    
    public static GraphExploreResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

}
