/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring.exporter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Base class for all monitoring documents.
 */
public abstract class MonitoringDoc implements ToXContentObject {

    private static final DateFormatter dateTimeFormatter = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    private final String cluster;
    private final long timestamp;
    private final long intervalMillis;
    private final Node node;
    private final MonitoredSystem system;
    private final String type;
    private final String id;

    public MonitoringDoc(
        final String cluster,
        final long timestamp,
        final long intervalMillis,
        @Nullable final Node node,
        final MonitoredSystem system,
        final String type,
        @Nullable final String id
    ) {

        this.cluster = Objects.requireNonNull(cluster);
        this.timestamp = timestamp;
        this.intervalMillis = intervalMillis;
        this.node = node;
        this.system = Objects.requireNonNull(system);
        this.type = Objects.requireNonNull(type);
        this.id = id;
    }

    public String getCluster() {
        return cluster;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }

    public Node getNode() {
        return node;
    }

    public MonitoredSystem getSystem() {
        return system;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
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
        return timestamp == that.timestamp
            && intervalMillis == that.intervalMillis
            && Objects.equals(cluster, that.cluster)
            && Objects.equals(node, that.node)
            && system == that.system
            && Objects.equals(type, that.type)
            && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, timestamp, intervalMillis, node, system, type, id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("cluster_uuid", cluster);
            builder.field("timestamp", toUTC(timestamp));
            builder.field("interval_ms", intervalMillis);
            builder.field("type", type);
            builder.field("source_node", node);
            innerToXContent(builder, params);
        }
        return builder.endObject();
    }

    protected abstract void innerToXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Converts a timestamp in milliseconds to its {@link String} representation in UTC time.
     *
     * @param timestamp the timestamp to convert
     * @return a string representing the timestamp
     */
    public static String toUTC(final long timestamp) {
        return dateTimeFormatter.formatMillis(timestamp);
    }

    /**
     * {@link Node} represents the node of the cluster from which the monitoring document
     * has been collected.
     */
    public static class Node implements Writeable, ToXContentObject {

        private final String uuid;
        private final String host;
        private final String transportAddress;
        private final String ip;
        private final String name;
        private final long timestamp;

        public Node(
            final String uuid,
            final String host,
            final String transportAddress,
            final String ip,
            final String name,
            final long timestamp
        ) {
            this.uuid = uuid;
            this.host = host;
            this.transportAddress = transportAddress;
            this.ip = ip;
            this.name = name;
            this.timestamp = timestamp;
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
            timestamp = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(uuid);
            out.writeOptionalString(host);
            out.writeOptionalString(transportAddress);
            out.writeOptionalString(ip);
            out.writeOptionalString(name);
            out.writeVLong(timestamp);
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

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("uuid", uuid);
                builder.field("host", host);
                builder.field("transport_address", transportAddress);
                builder.field("ip", ip);
                builder.field("name", name);
                builder.field("timestamp", toUTC(timestamp));
            }
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
                && Objects.equals(timestamp, node.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, host, transportAddress, ip, name, timestamp);
        }
    }
}
