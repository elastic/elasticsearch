/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.GRACE_PERIOD_ADDED_VERSION;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.REPLACE_SHUTDOWN_TYPE_ADDED_VERSION;
import static org.elasticsearch.core.Strings.format;

public class PutShutdownNodeAction extends ActionType<AcknowledgedResponse> {

    public static final PutShutdownNodeAction INSTANCE = new PutShutdownNodeAction();
    public static final String NAME = "cluster:admin/shutdown/create";

    public PutShutdownNodeAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String nodeId;
        private final SingleNodeShutdownMetadata.Type type;
        private final String reason;
        @Nullable
        private final TimeValue allocationDelay;
        @Nullable
        private final String targetNodeName;
        @Nullable
        private final TimeValue gracePeriod;

        private static final ParseField TYPE_FIELD = new ParseField("type");
        private static final ParseField REASON_FIELD = new ParseField("reason");
        private static final ParseField ALLOCATION_DELAY_FIELD = new ParseField("allocation_delay");
        private static final ParseField TARGET_NODE_FIELD = new ParseField("target_node_name");
        public static final ParseField GRACE_PERIOD_FIELD = new ParseField("grace_period");

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_node_shutdown_request",
            false,
            (a, nodeId) -> new Request(
                nodeId,
                SingleNodeShutdownMetadata.Type.parse((String) a[0]),
                (String) a[1],
                a[2] == null ? null : TimeValue.parseTimeValue((String) a[2], "put-shutdown-node-request-" + nodeId),
                (String) a[3],
                a[4] == null ? null : TimeValue.parseTimeValue((String) a[4], "put-shutdown-node-request-" + nodeId)
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ALLOCATION_DELAY_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TARGET_NODE_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), GRACE_PERIOD_FIELD);
        }

        public static Request parseRequest(String nodeId, XContentParser parser) {
            return PARSER.apply(parser, nodeId);
        }

        public Request(
            String nodeId,
            SingleNodeShutdownMetadata.Type type,
            String reason,
            @Nullable TimeValue allocationDelay,
            @Nullable String targetNodeName,
            @Nullable TimeValue gracePeriod
        ) {
            this.nodeId = nodeId;
            this.type = type;
            this.reason = reason;
            this.allocationDelay = allocationDelay;
            this.targetNodeName = targetNodeName;
            this.gracePeriod = gracePeriod;
        }

        public Request(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.type = in.readEnum(SingleNodeShutdownMetadata.Type.class);
            this.reason = in.readString();
            this.allocationDelay = in.readOptionalTimeValue();
            if (in.getTransportVersion().onOrAfter(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION)) {
                this.targetNodeName = in.readOptionalString();
            } else {
                this.targetNodeName = null;
            }
            if (in.getTransportVersion().onOrAfter(GRACE_PERIOD_ADDED_VERSION)) {
                this.gracePeriod = in.readOptionalTimeValue();
            } else {
                this.gracePeriod = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            if (out.getTransportVersion().before(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION)
                && this.type == SingleNodeShutdownMetadata.Type.REPLACE) {
                out.writeEnum(SingleNodeShutdownMetadata.Type.REMOVE);
            } else {
                out.writeEnum(type);
            }
            out.writeString(reason);
            out.writeOptionalTimeValue(allocationDelay);
            if (out.getTransportVersion().onOrAfter(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION)) {
                out.writeOptionalString(targetNodeName);
            }
            if (out.getTransportVersion().onOrAfter(GRACE_PERIOD_ADDED_VERSION)) {
                out.writeOptionalTimeValue(gracePeriod);
            }
        }

        public String getNodeId() {
            return nodeId;
        }

        public SingleNodeShutdownMetadata.Type getType() {
            return type;
        }

        public String getReason() {
            return reason;
        }

        public TimeValue getAllocationDelay() {
            return allocationDelay;
        }

        public String getTargetNodeName() {
            return targetNodeName;
        }

        public TimeValue getGracePeriod() {
            return gracePeriod;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException arve = new ActionRequestValidationException();

            if (Strings.hasText(nodeId) == false) {
                arve.addValidationError("the node id to shutdown is required");
            }

            if (type == null) {
                arve.addValidationError("the shutdown type is required");
            }

            if (Strings.hasText(nodeId) == false) {
                arve.addValidationError("the reason for shutdown is required");
            }

            if (allocationDelay != null && SingleNodeShutdownMetadata.Type.RESTART.equals(type) == false) {
                arve.addValidationError(ALLOCATION_DELAY_FIELD + " is only allowed for RESTART-type shutdown requests");
            }

            if (targetNodeName != null && type != SingleNodeShutdownMetadata.Type.REPLACE) {
                arve.addValidationError(
                    format(
                        "target node name is only valid for REPLACE type shutdowns, but was given type [%s] and target node name [%s]",
                        type,
                        targetNodeName
                    )
                );
            } else if (targetNodeName == null && type == SingleNodeShutdownMetadata.Type.REPLACE) {
                arve.addValidationError("target node name is required for REPLACE type shutdowns");
            }

            if (SingleNodeShutdownMetadata.Type.SIGTERM.equals(type)) {
                if (gracePeriod == null) {
                    arve.addValidationError("grace period is required for SIGTERM shutdowns");
                }
            } else if (gracePeriod != null) {
                arve.addValidationError(
                    format(
                        "grace period is only valid for SIGTERM type shutdowns, but was given type [%s] and target node name [%s]",
                        type,
                        targetNodeName
                    )
                );
            }

            if (arve.validationErrors().isEmpty() == false) {
                return arve;
            } else {
                return null;
            }
        }
    }
}
