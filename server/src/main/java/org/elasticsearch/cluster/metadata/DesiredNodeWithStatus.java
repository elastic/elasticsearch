/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record DesiredNodeWithStatus(DesiredNode desiredNode, Status status)
    implements
        Writeable,
        ToXContentObject,
        Comparable<DesiredNodeWithStatus> {

    private static final TransportVersion STATUS_TRACKING_SUPPORT_VERSION = TransportVersions.V_8_4_0;
    private static final ParseField STATUS_FIELD = new ParseField("status");

    public static final ConstructingObjectParser<DesiredNodeWithStatus, Void> PARSER = new ConstructingObjectParser<>(
        "desired_node_with_status",
        false,
        (args, unused) -> new DesiredNodeWithStatus(
            new DesiredNode(
                (Settings) args[0],
                (Processors) args[1],
                (DesiredNode.ProcessorsRange) args[2],
                (ByteSizeValue) args[3],
                (ByteSizeValue) args[4]
            ),
            // An unknown status is expected during upgrades to versions >= STATUS_TRACKING_SUPPORT_VERSION
            // the desired node status would be populated when a node in the newer version is elected as
            // master, the desired nodes status update happens in NodeJoinExecutor.
            args[5] == null ? Status.PENDING : (Status) args[5]
        )
    );

    static {
        DesiredNode.configureParser(PARSER);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Status.fromValue(p.shortValue()),
            STATUS_FIELD,
            ObjectParser.ValueType.INT
        );
    }

    public boolean pending() {
        return status == Status.PENDING;
    }

    public boolean actualized() {
        return status == Status.ACTUALIZED;
    }

    public String externalId() {
        return desiredNode.externalId();
    }

    public static DesiredNodeWithStatus readFrom(StreamInput in) throws IOException {
        final var desiredNode = DesiredNode.readFrom(in);
        final Status status;
        if (in.getTransportVersion().onOrAfter(STATUS_TRACKING_SUPPORT_VERSION)) {
            status = Status.fromValue(in.readShort());
        } else {
            // During upgrades, we consider all desired nodes as PENDING
            // since it's impossible to know if a node that was supposed to
            // join the cluster, it joined. The status will be updated
            // once the master node is upgraded to a version >= STATUS_TRACKING_SUPPORT_VERSION
            // in NodeJoinExecutor or when the desired nodes are upgraded to a new version.
            status = Status.PENDING;
        }
        return new DesiredNodeWithStatus(desiredNode, status);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        desiredNode.writeTo(out);
        if (out.getTransportVersion().onOrAfter(STATUS_TRACKING_SUPPORT_VERSION)) {
            out.writeShort(status.value);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final var desiredNodesSerializationContext = DesiredNodes.SerializationContext.valueOf(
            params.param(DesiredNodes.CONTEXT_MODE_PARAM, DesiredNodes.CONTEXT_MODE_CLUSTER_STATE)
        );
        builder.startObject();
        desiredNode.toInnerXContent(builder, params);
        // Only serialize the desired node status during cluster state serialization
        if (desiredNodesSerializationContext == DesiredNodes.SerializationContext.CLUSTER_STATE) {
            builder.field(STATUS_FIELD.getPreferredName(), status.value);
        }
        builder.endObject();
        return builder;
    }

    static DesiredNodeWithStatus fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public int compareTo(DesiredNodeWithStatus o) {
        return desiredNode.compareTo(o.desiredNode);
    }

    public boolean equalsWithProcessorsCloseTo(DesiredNodeWithStatus other) {
        return other != null && status == other.status && desiredNode.equalsWithProcessorsCloseTo(other.desiredNode);
    }

    public enum Status {
        PENDING((short) 0),
        ACTUALIZED((short) 1);

        private final short value;

        Status(short value) {
            this.value = value;
        }

        // visible for testing
        public short getValue() {
            return value;
        }

        static Status fromValue(short value) {
            return switch (value) {
                case 0 -> PENDING;
                case 1 -> ACTUALIZED;
                default -> throw new IllegalArgumentException("Unknown status " + value);
            };
        }
    }
}
