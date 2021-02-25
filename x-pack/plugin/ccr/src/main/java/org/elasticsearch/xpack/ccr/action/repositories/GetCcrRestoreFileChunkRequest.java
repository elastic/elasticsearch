/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

public class GetCcrRestoreFileChunkRequest extends ActionRequest implements RemoteClusterAwareRequest {

    private final DiscoveryNode node;
    private final String sessionUUID;
    private final String fileName;
    private final int size;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public GetCcrRestoreFileChunkRequest(DiscoveryNode node, String sessionUUID, String fileName, int size) {
        this.node = node;
        this.sessionUUID = sessionUUID;
        this.fileName = fileName;
        this.size = size;
        assert size > -1 : "The file chunk request size must be positive. Found: [" + size + "].";
    }

    GetCcrRestoreFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        node = null;
        sessionUUID = in.readString();
        fileName = in.readString();
        size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        out.writeString(fileName);
        out.writeVInt(size);
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    String getFileName() {
        return fileName;
    }

    int getSize() {
        return size;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        assert node != null : "Target node is null";
        return node;
    }
}
