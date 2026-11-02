/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * One resident cache region advertised by the source node of a search relocation.
 */
public final class CacheTransplantRegionInfo implements Writeable {

    private final long primaryTerm;
    private final String blobName;
    private final int region;
    private final int length;
    private final long timestampMillis;
    private final int frequency;

    public CacheTransplantRegionInfo(long primaryTerm, String blobName, int region, int length, long timestampMillis, int frequency) {
        this.primaryTerm = primaryTerm;
        this.blobName = blobName;
        this.region = region;
        this.length = length;
        this.timestampMillis = timestampMillis;
        this.frequency = frequency;
    }

    public CacheTransplantRegionInfo(StreamInput in) throws IOException {
        this.primaryTerm = in.readVLong();
        this.blobName = in.readString();
        this.region = in.readVInt();
        this.length = in.readVInt();
        this.timestampMillis = in.readLong();
        this.frequency = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeString(blobName);
        out.writeVInt(region);
        out.writeVInt(length);
        out.writeLong(timestampMillis);
        out.writeVInt(frequency);
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public String blobName() {
        return blobName;
    }

    public int region() {
        return region;
    }

    public int length() {
        return length;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public int frequency() {
        return frequency;
    }
}
