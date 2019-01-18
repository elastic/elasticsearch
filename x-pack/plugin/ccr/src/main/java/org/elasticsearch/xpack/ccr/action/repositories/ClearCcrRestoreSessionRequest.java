/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClearCcrRestoreSessionRequest extends BaseNodesRequest<ClearCcrRestoreSessionRequest> {

    private Request request;

    ClearCcrRestoreSessionRequest() {
    }

    public ClearCcrRestoreSessionRequest(String nodeId, Request request) {
        super(nodeId);
        this.request = request;
    }

    @Override
    public void readFrom(StreamInput streamInput) throws IOException {
        super.readFrom(streamInput);
        request = new Request();
        request.readFrom(streamInput);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        super.writeTo(streamOutput);
        request.writeTo(streamOutput);
    }

    public Request getRequest() {
        return request;
    }

    public static class Request extends BaseNodeRequest {

        private String sessionUUID;

        Request() {
        }

        public Request(String nodeId, String sessionUUID) {
            super(nodeId);
            this.sessionUUID = sessionUUID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            sessionUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionUUID);
        }

        public String getSessionUUID() {
            return sessionUUID;
        }
    }
}
