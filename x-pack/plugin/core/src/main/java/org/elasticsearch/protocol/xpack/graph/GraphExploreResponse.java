/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.graph.Connection.ConnectionId;
import org.elasticsearch.protocol.xpack.graph.Vertex.VertexId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Graph explore response holds a graph of {@link Vertex} and {@link Connection} objects
 * (nodes and edges in common graph parlance).
 *
 * @see GraphExploreRequest
 */
public class GraphExploreResponse extends ActionResponse implements ToXContentObject {

    private long tookInMillis;
    private boolean timedOut = false;
    private ShardOperationFailedException[] shardFailures = ShardSearchFailure.EMPTY_ARRAY;
    private Map<VertexId, Vertex> vertices;
    private Map<ConnectionId, Connection> connections;
    private boolean returnDetailedInfo;
    static final String RETURN_DETAILED_INFO_PARAM = "returnDetailedInfo";

    public GraphExploreResponse() {}

    public GraphExploreResponse(
        long tookInMillis,
        boolean timedOut,
        ShardOperationFailedException[] shardFailures,
        Map<VertexId, Vertex> vertices,
        Map<ConnectionId, Connection> connections,
        boolean returnDetailedInfo
    ) {
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

    public Collection<Vertex> getVertices() {
        return vertices.values();
    }

    public Vertex getVertex(VertexId id) {
        return vertices.get(id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

        Map<Vertex, Integer> vertexNumbers = new HashMap<>(vertices.size());

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

}
