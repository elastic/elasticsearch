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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

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
        private final TimeValue shardReallocationDelay;

        private static final ParseField TYPE_FIELD = new ParseField("type");
        private static final ParseField REASON_FIELD = new ParseField("reason");
        private static final ParseField REALLOCATION_DELAY_FIELD = new ParseField("shard_reallocation_delay");

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_node_shutdown_request",
            false,
            (a, nodeId) -> new Request(nodeId, SingleNodeShutdownMetadata.Type.parse((String) a[0]), (String) a[1], (TimeValue) a[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REALLOCATION_DELAY_FIELD);
        }

        public static Request parseRequest(String nodeId, XContentParser parser) {
            return PARSER.apply(parser, nodeId);
        }

        public Request(String nodeId, SingleNodeShutdownMetadata.Type type, String reason, @Nullable TimeValue shardReallocationDelay) {
            this.nodeId = nodeId;
            this.type = type;
            this.reason = reason;
            this.shardReallocationDelay = shardReallocationDelay;
        }

        public Request(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.type = in.readEnum(SingleNodeShutdownMetadata.Type.class);
            this.reason = in.readString();
            this.shardReallocationDelay = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeEnum(type);
            out.writeString(reason);
            out.writeOptionalTimeValue(shardReallocationDelay);
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

        public TimeValue getShardReallocationDelay() {
            return shardReallocationDelay;
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

            if (arve.validationErrors().isEmpty() == false) {
                return arve;
            } else {
                return null;
            }
        }
    }
}
