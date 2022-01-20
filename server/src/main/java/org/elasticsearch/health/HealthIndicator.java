/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class HealthIndicator implements VersionedNamedWriteable, ToXContentObject {

    public static final String COMPONENT_INFRASTRUCTURE = "infrastructure";
    public static final String COMPONENT_SNAPSHOTS = "snapshots";

    private final String name;
    private final Status status;
    private final String component;
    private final @Nullable String description;

    protected HealthIndicator(String name, String component, Status status, @Nullable String description) {
        this.name = Objects.requireNonNull(name);
        this.status = Objects.requireNonNull(status);
        this.component = Objects.requireNonNull(component);
        this.description = description;
    }

    protected HealthIndicator(StreamInput in) throws IOException {
        this.name = in.readString();
        this.status = Status.readFrom(in);
        this.component = in.readString();
        this.description = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        status.writeTo(out);
        out.writeString(component);
        out.writeOptionalString(description);
    }

    public String getName() {
        return name;
    }

    public Status getStatus() {
        return status;
    }

    public String getComponent() {
        return component;
    }

    @Override
    public String getWriteableName() {
        return getName();
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("indicator", name);
            builder.field("status", status.label);
            if (description != null) {
                builder.field("description", description);
            }
            builder.startObject("meta");
            {
                metaToXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder metaToXContent(XContentBuilder builder, Params params) throws IOException;

    protected enum Status implements Writeable {

        RED("red", (byte) 0),
        YELLOW("yellow", (byte) 1),
        GREEN("green", (byte) 2);

        private final String label;
        private final byte id;

        Status(String label, byte id) {
            this.label = Objects.requireNonNull(label);
            this.id = id;
        }

        public static Status from(String label) {
            return switch (label) {
                case "red" -> RED;
                case "yellow" -> YELLOW;
                case "green" -> GREEN;
                default -> throw new IllegalArgumentException("Status not found [label:" + label + "]");
            };
        }

        public static Status from(byte id) {
            return switch (id) {
                case (byte) 0 -> RED;
                case (byte) 1 -> YELLOW;
                case (byte) 2 -> GREEN;
                default -> throw new IllegalArgumentException("Status not found [id:" + id + "]");
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        static Status readFrom(StreamInput in) throws IOException {
            return from(in.readByte());
        }
    }
}
