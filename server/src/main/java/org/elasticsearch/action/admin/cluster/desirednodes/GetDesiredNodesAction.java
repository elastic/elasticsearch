/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetDesiredNodesAction extends ActionType<GetDesiredNodesAction.Response> {
    public static final GetDesiredNodesAction INSTANCE = new GetDesiredNodesAction();
    public static final String NAME = "cluster:admin/desired_nodes/get";

    GetDesiredNodesAction() {
        super(NAME, Response::new);
    }

    public static Request latestDesiredNodesRequest() {
        return new Request(Request.Mode.LATEST);
    }

    public static Request allDesiredNodesRequest() {
        return new Request(Request.Mode.ALL);
    }

    public static class Request extends MasterNodeReadRequest<Request> {
        public enum Mode {
            ALL((byte) 0),
            LATEST((byte) 1);

            private final byte value;

            Mode(byte value) {
                this.value = value;
            }

            static Mode fromValue(byte value) {
                return switch (value) {
                    case 0 -> ALL;
                    case 1 -> LATEST;
                    default -> throw new IllegalArgumentException("No Mode for value [" + value + "]");
                };
            }
        }

        private final Mode mode;

        Request(Mode mode) {
            this.mode = mode;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.mode = Mode.fromValue(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeByte(mode.value);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "Request{" + "mode=" + mode + '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final DesiredNodes desiredNodes;

        public Response(DesiredNodes desiredNodes) {
            this.desiredNodes = desiredNodes;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.desiredNodes = new DesiredNodes(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            desiredNodes.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("desired_nodes", desiredNodes);
            builder.endObject();
            return builder;
        }
    }
}
