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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeleteShutdownNodeAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteShutdownNodeAction INSTANCE = new DeleteShutdownNodeAction();
    public static final String NAME = "cluster:admin/shutdown/delete";

    public DeleteShutdownNodeAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String nodeId;

        public Request(String nodeId) {
            this.nodeId = nodeId;
        }

        public Request(StreamInput in) throws IOException {
            this.nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.hasText(nodeId) == false) {
                ActionRequestValidationException arve = new ActionRequestValidationException();
                arve.addValidationError("the node id to remove from shutdown is required");
                return arve;
            }
            return null;
        }
    }

}
