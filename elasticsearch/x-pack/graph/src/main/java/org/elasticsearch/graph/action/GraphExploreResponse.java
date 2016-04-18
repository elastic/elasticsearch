/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph.action;

import com.carrotsearch.hppc.ObjectIntHashMap;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.graph.action.Connection.ConnectionId;
import org.elasticsearch.graph.action.Vertex.VertexId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.search.ShardSearchFailure.readShardSearchFailure;

/**
 * Graph explore response holds a graph of {@link Vertex} and {@link Connection} objects 
 * (nodes and edges in common graph parlance).
 * 
 * @see GraphExploreRequest
 */
public class GraphExploreResponse extends ActionResponse implements ToXContent {

    private long tookInMillis;
    private boolean timedOut = false;
    private ShardOperationFailedException[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;
    private Map<VertexId, Vertex> vertices;
    private Map<ConnectionId, Connection> connections;
    private boolean returnDetailedInfo;
    static final String RETURN_DETAILED_INFO_PARAM = "returnDetailedInfo";

    GraphExploreResponse() {
    }

    GraphExploreResponse(long tookInMillis, boolean timedOut, ShardOperationFailedException[] shardFailures, Map<VertexId, Vertex> vertices,
            Map<ConnectionId, Connection> connections, boolean returnDetailedInfo) {
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        timedOut = in.readBoolean();

        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        // read vertices
        size = in.readVInt();
        vertices = new HashMap<>();
        for (int i = 0; i < size; i++) {
            Vertex n = Vertex.readFrom(in);
            vertices.put(n.getId(), n);
        }

        size = in.readVInt();

        connections = new HashMap<>();
        for (int i = 0; i < size; i++) {
            Connection e = new Connection();
            e.readFrom(in, vertices);
            connections.put(e.getId(), e);
        }
        
        returnDetailedInfo = in.readBoolean();

    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Collection<Vertex> getVertices() {
        return vertices.values();
    }
    
    public Vertex getVertex(VertexId id) {
        return vertices.get(id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeBoolean(timedOut);

        out.writeVInt(shardFailures.length);
        for (ShardOperationFailedException shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        out.writeVInt(vertices.size());
        for (Vertex vertex : vertices.values()) {
            vertex.writeTo(out);
        }

        out.writeVInt(connections.size());
        for (Connection connection : connections.values()) {
            connection.writeTo(out);
        }
        
        out.writeBoolean(returnDetailedInfo);

    }

    static final class Fields {
        static final String TOOK = "took";
        static final String TIMED_OUT = "timed_out";
        static final String INDICES = "_indices";
        static final String FAILURES = "failures";
        static final String VERTICES = "vertices";
        static final String CONNECTIONS = "connections";

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookInMillis);
        builder.field(Fields.TIMED_OUT, timedOut);

        builder.startArray(Fields.FAILURES);
        if (shardFailures != null) {
            for (ShardOperationFailedException shardFailure : shardFailures) {
                builder.startObject();
                shardFailure.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();

        ObjectIntHashMap<Vertex> vertexNumbers = new ObjectIntHashMap<>(vertices.size());
        
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put(RETURN_DETAILED_INFO_PARAM, Boolean.toString(returnDetailedInfo));
        Params extendedParams = new DelegatingMapParams(extraParams, params);
        
        builder.startArray(Fields.VERTICES);
        for (Vertex vertex : vertices.values()) {
            builder.startObject();
            vertexNumbers.put(vertex, vertexNumbers.size());
            vertex.toXContent(builder, extendedParams);            
            builder.endObject();
        }
        builder.endArray();

        builder.startArray(Fields.CONNECTIONS);
        for (Connection connection : connections.values()) {
            builder.startObject();
            connection.toXContent(builder, extendedParams, vertexNumbers);
            builder.endObject();
        }
        builder.endArray();

        return builder;
    }


}
