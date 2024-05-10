/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class DeleteShutdownNodeAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteShutdownNodeAction INSTANCE = new DeleteShutdownNodeAction();
    public static final String NAME = "cluster:admin/shutdown/delete";

    public DeleteShutdownNodeAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String nodeId;

        public Request(String nodeId) {
            this.nodeId = nodeId;
        }

        public Request(StreamInput in) throws IOException {
            if (in.getTransportVersion().isPatchFrom(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_13)
                || in.getTransportVersion().isPatchFrom(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_14)
                || in.getTransportVersion().onOrAfter(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX)) {
                // effectively super(in):
                setParentTask(TaskId.readFromStream(in));
                masterNodeTimeout(in.readTimeValue());
                ackTimeout(in.readTimeValue());
            }
            this.nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().isPatchFrom(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_13)
                || out.getTransportVersion().isPatchFrom(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_14)
                || out.getTransportVersion().onOrAfter(TransportVersions.SHUTDOWN_REQUEST_TIMEOUTS_FIX)) {
                super.writeTo(out);
            }
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
