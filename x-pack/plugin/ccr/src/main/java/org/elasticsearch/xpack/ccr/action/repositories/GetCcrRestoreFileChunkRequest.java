/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    private DiscoveryNode node;
    private final String sessionUUID;
    private final String fileName;
    private final long offset;
    private final int size;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public GetCcrRestoreFileChunkRequest(DiscoveryNode node, String sessionUUID, String fileName, long offset, int size) {
        this.sessionUUID = sessionUUID;
        this.node = node;
        this.fileName = fileName;
        this.offset = offset;
        this.size = size;
    }

    GetCcrRestoreFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        sessionUUID = in.readString();
        fileName = in.readString();
        offset = in.readVLong();
        size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        out.writeString(fileName);
        out.writeVLong(offset);
        out.writeVInt(size);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public String getSessionUUID() {
        return sessionUUID;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        return node;
    }
}
