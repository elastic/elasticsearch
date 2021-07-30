/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.transport.RemoteClusterAwareRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClearCcrRestoreSessionRequest extends ActionRequest implements RemoteClusterAwareRequest {

    private DiscoveryNode node;
    private String sessionUUID;

    ClearCcrRestoreSessionRequest(StreamInput in) throws IOException {
        super(in);
        sessionUUID = in.readString();
    }

    public ClearCcrRestoreSessionRequest(String sessionUUID, DiscoveryNode node) {
        this.sessionUUID = sessionUUID;
        this.node = node;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        return node;
    }
}
