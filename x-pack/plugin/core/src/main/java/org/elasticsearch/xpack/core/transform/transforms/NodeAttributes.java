/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A Pojo class containing an Elastic Node's attributes
 */
public class NodeAttributes implements ToXContentObject, Writeable {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField NAME = new ParseField("name");
    public static final ParseField EPHEMERAL_ID = new ParseField("ephemeral_id");
    public static final ParseField TRANSPORT_ADDRESS = new ParseField("transport_address");
    public static final ParseField ATTRIBUTES = new ParseField("attributes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodeAttributes, Void> PARSER =
        new ConstructingObjectParser<>("node", true,
            (a) -> {
                int i = 0;
                String id = (String) a[i++];
                String name = (String) a[i++];
                String ephemeralId = (String) a[i++];
                String transportAddress = (String) a[i++];
                Map<String, String> attributes = (Map<String, String>) a[i];
                return new NodeAttributes(id, name, ephemeralId, transportAddress, attributes);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), EPHEMERAL_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TRANSPORT_ADDRESS);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.mapStrings(),
            ATTRIBUTES,
            ObjectParser.ValueType.OBJECT);
    }

    private final String id;
    private final String name;
    private final String ephemeralId;
    private final String transportAddress;
    private final Map<String, String> attributes;

    public static NodeAttributes fromDiscoveryNode(DiscoveryNode node) {
        return new NodeAttributes(node.getId(),
            node.getName(),
            node.getEphemeralId(),
            node.getAddress().toString(),
            // TODO add data_frame attributes when/if they are added
            Collections.emptyMap());
    }

    public NodeAttributes(String id, String name, String ephemeralId, String transportAddress, Map<String, String> attributes) {
        this.id = ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
        this.name = ExceptionsHelper.requireNonNull(name, NAME.getPreferredName());
        this.ephemeralId = ExceptionsHelper.requireNonNull(ephemeralId, EPHEMERAL_ID.getPreferredName());
        this.transportAddress = ExceptionsHelper.requireNonNull(transportAddress, TRANSPORT_ADDRESS.getPreferredName());
        this.attributes = Collections.unmodifiableMap(ExceptionsHelper.requireNonNull(attributes, ATTRIBUTES.getPreferredName()));
    }

    public NodeAttributes(StreamInput in) throws IOException {
        this.id = in.readString();
        this.name = in.readString();
        this.ephemeralId = in.readString();
        this.transportAddress = in.readString();
        this.attributes = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    /**
     * The unique identifier of the node.
     */
    public String getId() {
        return id;
    }

    /**
     * The node name.
     */
    public String getName() {
        return name;
    }

    /**
     * The ephemeral id of the node.
     */
    public String getEphemeralId() {
        return ephemeralId;
    }

    /**
     * The host and port where transport HTTP connections are accepted.
     */
    public String getTransportAddress() {
        return transportAddress;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(NAME.getPreferredName(), name);
        builder.field(EPHEMERAL_ID.getPreferredName(), ephemeralId);
        builder.field(TRANSPORT_ADDRESS.getPreferredName(), transportAddress);
        builder.field(ATTRIBUTES.getPreferredName(), attributes);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, ephemeralId, transportAddress, attributes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        NodeAttributes that = (NodeAttributes) other;
        return Objects.equals(id, that.id) &&
            Objects.equals(name, that.name) &&
            Objects.equals(ephemeralId, that.ephemeralId) &&
            Objects.equals(transportAddress, that.transportAddress) &&
            Objects.equals(attributes, that.attributes);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(name);
        out.writeString(ephemeralId);
        out.writeString(transportAddress);
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
    }
}
