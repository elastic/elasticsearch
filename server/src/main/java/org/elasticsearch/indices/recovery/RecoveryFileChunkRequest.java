/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;

public final class RecoveryFileChunkRequest extends RecoveryTransportRequest implements RefCounted {
    private final boolean lastChunk;
    private final long recoveryId;
    private final ShardId shardId;
    private final long position;
    private final ReleasableBytesReference content;
    private final StoreFileMetadata metadata;
    private final long sourceThrottleTimeInNanos;

    private final int totalTranslogOps;

    public RecoveryFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        final String name = in.readString();
        position = in.readVLong();
        final long length = in.readVLong();
        final String checksum = in.readString();
        content = in.readReleasableBytesReference();
        final String writtenBy = in.readString();
        metadata = new StoreFileMetadata(name, length, checksum, writtenBy);
        lastChunk = in.readBoolean();
        totalTranslogOps = in.readVInt();
        sourceThrottleTimeInNanos = in.readLong();
    }

    public RecoveryFileChunkRequest(long recoveryId, final long requestSeqNo, ShardId shardId, StoreFileMetadata metadata, long position,
                                    ReleasableBytesReference content, boolean lastChunk, int totalTranslogOps,
                                    long sourceThrottleTimeInNanos) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.metadata = metadata;
        this.position = position;
        this.content = content.retain();
        this.lastChunk = lastChunk;
        this.totalTranslogOps = totalTranslogOps;
        this.sourceThrottleTimeInNanos = sourceThrottleTimeInNanos;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String name() {
        return metadata.name();
    }

    public long position() {
        return position;
    }

    public long length() {
        return metadata.length();
    }

    public ReleasableBytesReference content() {
        return content;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long sourceThrottleTimeInNanos() {
        return sourceThrottleTimeInNanos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeString(metadata.name());
        out.writeVLong(position);
        out.writeVLong(metadata.length());
        out.writeString(metadata.checksum());
        out.writeBytesReference(content);
        out.writeString(metadata.writtenBy());
        out.writeBoolean(lastChunk);
        out.writeVInt(totalTranslogOps);
        out.writeLong(sourceThrottleTimeInNanos);
    }

    @Override
    public String toString() {
        return shardId + ": name='" + name() + '\'' +
                ", position=" + position +
                ", length=" + length();
    }

    public StoreFileMetadata metadata() {
        return metadata;
    }

    /**
     * Returns <code>true</code> if this chunk is the last chunk in the stream.
     */
    public boolean lastChunk() {
        return lastChunk;
    }

    @Override
    public void incRef() {
        content.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return content.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return content.decRef();
    }
}
