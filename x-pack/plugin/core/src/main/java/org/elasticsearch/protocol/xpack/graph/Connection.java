/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.protocol.xpack.graph.Vertex.VertexId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A Connection links exactly two {@link Vertex} objects. The basis of a
 * connection is one or more documents have been found that contain
 * this pair of terms and the strength of the connection is recorded
 * as a weight.
 */
public class Connection {
    private Vertex from;
    private Vertex to;
    private double weight;
    private long docCount;

    public Connection(Vertex from, Vertex to, double weight, long docCount) {
        this.from = from;
        this.to = to;
        this.weight = weight;
        this.docCount = docCount;
    }

    public Connection(StreamInput in, Map<VertexId, Vertex> vertices) throws IOException {
        from = vertices.get(new VertexId(in.readString(), in.readString()));
        to = vertices.get(new VertexId(in.readString(), in.readString()));
        weight = in.readDouble();
        docCount = in.readVLong();
    }

    Connection() {}

    void writeTo(StreamOutput out) throws IOException {
        out.writeString(from.getField());
        out.writeString(from.getTerm());
        out.writeString(to.getField());
        out.writeString(to.getTerm());
        out.writeDouble(weight);
        out.writeVLong(docCount);
    }

    public ConnectionId getId() {
        return new ConnectionId(from.getId(), to.getId());
    }

    public Vertex getFrom() {
        return from;
    }

    public Vertex getTo() {
        return to;
    }

    /**
     * @return a measure of the relative connectedness between a pair of {@link Vertex} objects
     */
    public double getWeight() {
        return weight;
    }

    /**
     * @return the number of documents in the sampled set that contained this
     * pair of {@link Vertex} objects.
     */
    public long getDocCount() {
        return docCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Connection other = (Connection) obj;
        return docCount == other.docCount && weight == other.weight && Objects.equals(to, other.to) && Objects.equals(from, other.from);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docCount, weight, from, to);
    }

    private static final ParseField SOURCE = new ParseField("source");
    private static final ParseField TARGET = new ParseField("target");
    private static final ParseField WEIGHT = new ParseField("weight");
    private static final ParseField DOC_COUNT = new ParseField("doc_count");

    void toXContent(XContentBuilder builder, Params params, Map<Vertex, Integer> vertexNumbers) throws IOException {
        builder.field(SOURCE.getPreferredName(), vertexNumbers.get(from));
        builder.field(TARGET.getPreferredName(), vertexNumbers.get(to));
        builder.field(WEIGHT.getPreferredName(), weight);
        builder.field(DOC_COUNT.getPreferredName(), docCount);
    }

    /**
     * An identifier (implements hashcode and equals) that represents a
     * unique key for a {@link Connection}
     */
    public static class ConnectionId {
        private final VertexId source;
        private final VertexId target;

        public ConnectionId(VertexId source, VertexId target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConnectionId that = (ConnectionId) o;
            return Objects.equals(source, that.source) && Objects.equals(target, that.target);
        }

        @Override
        public int hashCode() {
            int result = source != null ? source.hashCode() : 0;
            result = 31 * result + (target != null ? target.hashCode() : 0);
            return result;
        }

        public VertexId getSource() {
            return source;
        }

        public VertexId getTarget() {
            return target;
        }

        @Override
        public String toString() {
            return getSource() + "->" + getTarget();
        }
    }
}
