/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all monitoring documents.
 */
public class MonitoringDoc implements Writeable {

    private final String monitoringId;
    private final String monitoringVersion;

    private String clusterUUID;
    private long timestamp;
    private Node sourceNode;

    public MonitoringDoc(String monitoringId, String monitoringVersion) {
        this.monitoringId = monitoringId;
        this.monitoringVersion = monitoringVersion;
    }

    /**
     * Read from a stream.
     */
    public MonitoringDoc(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
        clusterUUID = in.readOptionalString();
        timestamp = in.readVLong();
        sourceNode = in.readOptionalWriteable(Node::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(getMonitoringId());
        out.writeOptionalString(getMonitoringVersion());
        out.writeOptionalString(getClusterUUID());
        out.writeVLong(getTimestamp());
        out.writeOptionalWriteable(getSourceNode());
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

    public static class Node implements Writeable, ToXContent {

        private String uuid;
        private String host;
        private String transportAddress;
        private String ip;
        private String name;
        private Map<String, String> attributes;

        public Node(String uuid, String host, String transportAddress, String ip, String name,
                    Map<String, String> attributes) {
            this.uuid = uuid;
            this.host = host;
            this.transportAddress = transportAddress;
            this.ip = ip;
            this.name = name;
            if (attributes == null) {
                this.attributes = new HashMap<>();
            } else {
                this.attributes = Collections.unmodifiableMap(attributes);
            }
        }

        /**
         * Read from a stream.
         */
        public Node(StreamInput in) throws IOException {
            uuid = in.readOptionalString();
            host = in.readOptionalString();
            transportAddress = in.readOptionalString();
            ip = in.readOptionalString();
            name = in.readOptionalString();
            int size = in.readVInt();
            this.attributes = new HashMap<>();
            for (int i = 0; i < size; i++) {
                attributes.put(in.readString(), in.readString());
            }
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
                for (Map.Entry<String, String> entry : attributes.entrySet()) {
                    out.writeString(entry.getKey());
                    out.writeString(entry.getValue());
                }
            } else {
                out.writeVInt(0);
            }
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

        public Map<String, String> getAttributes() {
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
            for (Map.Entry<String, String> entry : getAttributes().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return builder.endObject();
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
            static final String UUID = "uuid";
            static final String HOST = "host";
            static final String TRANSPORT_ADDRESS = "transport_address";
            static final String IP = "ip";
            static final String NAME = "name";
            static final String ATTRIBUTES = "attributes";
        }
    }
}