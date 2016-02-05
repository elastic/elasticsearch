/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public abstract class MarvelDoc {

    private String index;
    private String type;
    private String id;

    private String clusterUUID;
    private long timestamp;

    private Node sourceNode;

    public MarvelDoc() {
    }

    public MarvelDoc(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public void setClusterUUID(String clusterUUID) {
        this.clusterUUID = clusterUUID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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

    public static class Node implements ToXContent {

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
            this.attributes = attributes;
        }

        public String getUUID() {
            return uuid;
        }

        public void setUUID(String uuid) {
            this.uuid = uuid;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getTransportAddress() {
            return transportAddress;
        }

        public void setTransportAddress(String transportAddress) {
            this.transportAddress = transportAddress;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ImmutableOpenMap<String, String> getAttributes() {
            return attributes;
        }

        public void setAttributes(ImmutableOpenMap<String, String> attributes) {
            this.attributes = attributes;
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