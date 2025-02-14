/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Statistics about an index feature.
 */
public class SynonymsStats implements ToXContentObject, Writeable {

    int count;
    int indexCount;

    SynonymsStats() {}

    SynonymsStats(StreamInput in) throws IOException {
        this.count = in.readVInt();
        this.indexCount = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(count);
        out.writeVInt(indexCount);
    }

    /**
     * Return the number of times this synonym type is used across the cluster.
     */
    public int getCount() {
        return count;
    }

    /**
     * Return the number of indices that use this synonym type across the cluster.
     */
    public int getIndexCount() {
        return indexCount;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SynonymsStats == false) {
            return false;
        }
        SynonymsStats that = (SynonymsStats) other;
        return count == that.count && indexCount == that.indexCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, indexCount);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("count", count);
        builder.field("index_count", indexCount);
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

    @Override
    public String toString() {
        return "SynonymsStats{count=" + count + ", indexCount=" + indexCount + '}';
    }
}
