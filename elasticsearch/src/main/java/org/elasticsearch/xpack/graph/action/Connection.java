/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph.action;

import com.carrotsearch.hppc.ObjectIntHashMap;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.xpack.graph.action.Vertex.VertexId;

import java.io.IOException;
import java.util.Map;

/**
 * A Connection links exactly two {@link Vertex} objects. The basis of a 
 * connection is one or more documents have been found that contain
 * this pair of terms and the strength of the connection is recorded
 * as a weight.
 */
public class Connection {
    Vertex from;
    Vertex to;
    double weight;
    long docCount;

    Connection(Vertex from, Vertex to, double weight, long docCount) {
        this.from = from;
        this.to = to;
        this.weight = weight;
        this.docCount = docCount;
    }

    void readFrom(StreamInput in, Map<VertexId, Vertex> vertices) throws IOException {
        from = vertices.get(new VertexId(in.readString(), in.readString()));
        to = vertices.get(new VertexId(in.readString(), in.readString()));
        weight = in.readDouble();
        docCount = in.readVLong();
    }

    Connection() {
    }

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

    void toXContent(XContentBuilder builder, Params params, ObjectIntHashMap<Vertex> vertexNumbers) throws IOException {
        builder.field("source", vertexNumbers.get(from));
        builder.field("target", vertexNumbers.get(to));
        builder.field("weight", weight);
        builder.field("doc_count", docCount);
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
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ConnectionId vertexId = (ConnectionId) o;

            if (source != null ? !source.equals(vertexId.source) : vertexId.source != null)
                return false;
            if (target != null ? !target.equals(vertexId.target) : vertexId.target != null)
                return false;

            return true;
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
