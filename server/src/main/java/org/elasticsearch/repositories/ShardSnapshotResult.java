/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * The details of a successful shard-level snapshot that are used to build the overall snapshot during finalization.
 */
public class ShardSnapshotResult implements Writeable {

    private final ShardGeneration generation;

    private final ByteSizeValue size;

    private final int segmentCount;

    /**
     * @param generation   the shard generation UUID, which uniquely identifies the specific snapshot of the shard
     * @param size         the total size of all the blobs that make up the shard snapshot, or equivalently, the size of the shard when
     *                     restored
     * @param segmentCount the number of segments in this shard snapshot
     */
    public ShardSnapshotResult(ShardGeneration generation, ByteSizeValue size, int segmentCount) {
        this.generation = Objects.requireNonNull(generation);
        this.size = Objects.requireNonNull(size);
        assert segmentCount >= 0;
        this.segmentCount = segmentCount;
    }

    public ShardSnapshotResult(StreamInput in) throws IOException {
        generation = new ShardGeneration(in);
        size = new ByteSizeValue(in);
        segmentCount = in.readVInt();
    }

    /**
     * @return the shard generation UUID, which uniquely identifies the specific snapshot of the shard
     */
    public ShardGeneration getGeneration() {
        return generation;
    }

    /**
     * @return the total size of all the blobs that make up the shard snapshot, or equivalently, the size of the shard when restored
     */
    public ByteSizeValue getSize() {
        return size;
    }

    /**
     * @return the number of segments in this shard snapshot
     */
    public int getSegmentCount() {
        return segmentCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        generation.writeTo(out);
        size.writeTo(out);
        out.writeVInt(segmentCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardSnapshotResult that = (ShardSnapshotResult) o;
        return segmentCount == that.segmentCount && generation.equals(that.generation) && size.equals(that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, size, segmentCount);
    }

    @Override
    public String toString() {
        return "ShardSnapshotResult{" + "generation='" + generation + '\'' + ", size=" + size + ", segmentCount=" + segmentCount + '}';
    }
}
