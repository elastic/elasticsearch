/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A Pojo class containing an Elastic Node's attributes
 */
public class NodeAttributes implements ToXContentObject {

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

    public NodeAttributes(String id, String name, String ephemeralId, String transportAddress, Map<String, String> attributes) {
        this.id = id;
        this.name = name;
        this.ephemeralId = ephemeralId;
        this.transportAddress = transportAddress;
        this.attributes = Collections.unmodifiableMap(attributes);
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

    /**
     * Additional attributes related to this node
     */
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
}
