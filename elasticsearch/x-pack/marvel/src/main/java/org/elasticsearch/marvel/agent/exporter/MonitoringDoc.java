/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Base class for all monitoring documents.
 */
public class MonitoringDoc implements Writeable<MonitoringDoc> {

    private final String monitoringId;
    private final String monitoringVersion;

    private String clusterUUID;
    private long timestamp;
    private Node sourceNode;

    public MonitoringDoc(String monitoringId, String monitoringVersion) {
        this.monitoringId = monitoringId;
        this.monitoringVersion = monitoringVersion;
    }

    public MonitoringDoc(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
        clusterUUID = in.readOptionalString();
        timestamp = in.readVLong();
        sourceNode = in.readOptionalWritable(Node::new);
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public void setClusterUUID(String clusterUUID) {
        this.clusterUUID = clusterUUID;
    }

    public String getMonitoringId() {
        return monitoringId;
    }

    public String getMonitoringVersion() {
        return monitoringVersion;
    }

    public Node getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(Node node) {
        this.sourceNode = node;
    }

    public void setSourceNode(DiscoveryNode node) {
        setSourceNode(new Node(node.getId(), node.getHostName(), node.getAddress().toString(),
                node.getHostAddress(), node.getName(), node.getAttributes()));
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "monitoring document [class=" + getClass().getSimpleName() +
                ", monitoring id=" + getMonitoringId() +
                ", monitoring version=" + getMonitoringVersion() +
                "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(getMonitoringId());
        out.writeOptionalString(getMonitoringVersion());
        out.writeOptionalString(getClusterUUID());
        out.writeVLong(getTimestamp());
        out.writeOptionalWriteable(getSourceNode());
    }

    @Override
    public MonitoringDoc readFrom(StreamInput in) throws IOException {
        return new MonitoringDoc(in);
    }

    public static class Node implements Writeable<Node>, ToXContent {

        private String uuid;
        private String host;
        private String transportAddress;
        private String ip;
        private String name;
        private ImmutableOpenMap<String, String> attributes;

        public Node(String uuid, String host, String transportAddress, String ip, String name,
                    ImmutableOpenMap<String, String> attributes) {
            this.uuid = uuid;
            this.host = host;
            this.transportAddress = transportAddress;
            this.ip = ip;
            this.name = name;

            ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.builder();
            if (attributes != null) {
                for (ObjectObjectCursor<String, String> entry : attributes) {
                    builder.put(entry.key.intern(), entry.value.intern());
                }
            }
            this.attributes = builder.build();
        }

        public Node(StreamInput in) throws IOException {
            uuid = in.readOptionalString();
            host = in.readOptionalString();
            transportAddress = in.readOptionalString();
            ip = in.readOptionalString();
            name = in.readOptionalString();
            int size = in.readVInt();
            ImmutableOpenMap.Builder<String, String> attributes = ImmutableOpenMap.builder(size);
            for (int i = 0; i < size; i++) {
                attributes.put(in.readOptionalString(), in.readOptionalString());
            }
            this.attributes = attributes.build();
        }

        public String getUUID() {
            return uuid;
        }

        public String getHost() {
            return host;
        }

        public String getTransportAddress() {
            return transportAddress;
        }

        public String getIp() {
            return ip;
        }

        public String getName() {
            return name;
        }

        public ImmutableOpenMap<String, String> getAttributes() {
            return attributes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.UUID, getUUID());
            builder.field(Fields.HOST, getHost());
            builder.field(Fields.TRANSPORT_ADDRESS, getTransportAddress());
            builder.field(Fields.IP, getIp());
            builder.field(Fields.NAME, getName());

            builder.startObject(Fields.ATTRIBUTES);
            for (ObjectObjectCursor<String, String> attr : getAttributes()) {
                builder.field(attr.key, attr.value);
            }
            builder.endObject();
            return builder.endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(uuid);
            out.writeOptionalString(host);
            out.writeOptionalString(transportAddress);
            out.writeOptionalString(ip);
            out.writeOptionalString(name);
            if (attributes != null) {
                out.writeVInt(attributes.size());
                for (ObjectObjectCursor<String, String> entry : attributes) {
                    out.writeOptionalString(entry.key);
                    out.writeOptionalString(entry.value);
                }
            } else {
                out.writeVInt(0);
            }
        }

        @Override
        public Node readFrom(StreamInput in) throws IOException {
            return new Node(in);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            if (uuid != null ? !uuid.equals(node.uuid) : node.uuid != null) return false;
            if (host != null ? !host.equals(node.host) : node.host != null) return false;
            if (transportAddress != null ? !transportAddress.equals(node.transportAddress) : node.transportAddress != null) return false;
            if (ip != null ? !ip.equals(node.ip) : node.ip != null) return false;
            if (name != null ? !name.equals(node.name) : node.name != null) return false;
            return !(attributes != null ? !attributes.equals(node.attributes) : node.attributes != null);

        }

        @Override
        public int hashCode() {
            int result = uuid != null ? uuid.hashCode() : 0;
            result = 31 * result + (host != null ? host.hashCode() : 0);
            result = 31 * result + (transportAddress != null ? transportAddress.hashCode() : 0);
            result = 31 * result + (ip != null ? ip.hashCode() : 0);
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
            return result;
        }

        static final class Fields {
            static final XContentBuilderString UUID = new XContentBuilderString("uuid");
            static final XContentBuilderString HOST = new XContentBuilderString("host");
            static final XContentBuilderString TRANSPORT_ADDRESS = new XContentBuilderString("transport_address");
            static final XContentBuilderString IP = new XContentBuilderString("ip");
            static final XContentBuilderString NAME = new XContentBuilderString("name");
            static final XContentBuilderString ATTRIBUTES = new XContentBuilderString("attributes");
        }
    }
}