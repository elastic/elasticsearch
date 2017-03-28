/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc.Node.fromDiscoveryNode;

/**
 * Base class for all monitoring documents.
 */
public class MonitoringDoc {

    @Deprecated private final String monitoringId;
    @Deprecated private final String monitoringVersion;

    private final String type;
    private final String id;

    private final String clusterUUID;
    private final long timestamp;
    private final Node sourceNode;

    private final BytesReference source;
    private final XContentType sourceXContentType;

    protected MonitoringDoc(String monitoringId, String monitoringVersion, String type, String id,
                            String clusterUUID, long timestamp, Node node,
                            BytesReference source, XContentType sourceXContentType) {
        this.monitoringId = monitoringId;
        this.monitoringVersion = monitoringVersion;
        this.type = type;
        this.id = id;
        this.clusterUUID = clusterUUID;
        this.timestamp = timestamp;
        this.sourceNode = node;
        this.source = source;
        this.sourceXContentType = sourceXContentType;
    }

    public MonitoringDoc(String monitoringId, String monitoringVersion, String type, String id,
                            String clusterUUID, long timestamp, Node node) {
        this(monitoringId, monitoringVersion, type, id, clusterUUID, timestamp, node, null, null);
    }

    public MonitoringDoc(String monitoringId, String monitoringVersion, String type, String id,
                         String clusterUUID, long timestamp, DiscoveryNode discoveryNode) {
        this(monitoringId, monitoringVersion, type, id, clusterUUID, timestamp,
                fromDiscoveryNode(discoveryNode));
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    @Deprecated
    public String getMonitoringId() {
        return monitoringId;
    }

    @Deprecated
    public String getMonitoringVersion() {
        return monitoringVersion;
    }

    public Node getSourceNode() {
        return sourceNode;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return sourceXContentType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MonitoringDoc that = (MonitoringDoc) o;
        return timestamp == that.timestamp &&
                Objects.equals(monitoringId, that.monitoringId)
                && Objects.equals(monitoringVersion, that.monitoringVersion)
                && Objects.equals(type, that.type)
                && Objects.equals(id, that.id)
                && Objects.equals(clusterUUID, that.clusterUUID)
                && Objects.equals(sourceNode, that.sourceNode)
                && Objects.equals(sourceNode, that.sourceNode)
                && Objects.equals(sourceXContentType, that.sourceXContentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(monitoringId, monitoringVersion, type, id, clusterUUID, timestamp,
                sourceNode, source, sourceXContentType);
    }

    /**
     * {@link Node} represents the node of the cluster from which the monitoring document
     * has been collected.
     */
    public static class Node implements Writeable,ToXContent {

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Node node = (Node) o;
            return Objects.equals(uuid, node.uuid)
                    && Objects.equals(host, node.host)
                    && Objects.equals(transportAddress, node.transportAddress)
                    && Objects.equals(ip, node.ip)
                    && Objects.equals(name, node.name)
                    && Objects.equals(attributes, node.attributes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, host, transportAddress, ip, name, attributes);
        }

        /**
         * Creates a {@link MonitoringDoc.Node} from a {@link DiscoveryNode}, copying over the
         * required information.
         *
         * @param discoveryNode the {@link DiscoveryNode}
         * @return a {@link MonitoringDoc.Node} instance, or {@code null} if the given discovery
         *         node is null.
         */
        static Node fromDiscoveryNode(@Nullable DiscoveryNode discoveryNode) {
            MonitoringDoc.Node node = null;
            if (discoveryNode != null) {
                node = new MonitoringDoc.Node(discoveryNode.getId(), discoveryNode.getHostName(),
                        discoveryNode.getAddress().toString(), discoveryNode.getHostAddress(),
                        discoveryNode.getName(), discoveryNode.getAttributes());
            }
            return node;
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