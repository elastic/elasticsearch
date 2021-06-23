/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Statistics about an index feature.
 */
public class IndexFeatureStats implements ToXContentObject, Writeable {

    final String name;
    int count;
    int indexCount;

    IndexFeatureStats(String name) {
        this.name = Objects.requireNonNull(name);
    }

    IndexFeatureStats(StreamInput in) throws IOException {
        this.name = in.readString();
        this.count = in.readVInt();
        this.indexCount = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(count);
        out.writeVInt(indexCount);
    }

    /**
     * Return the name of the field type.
     */
    public String getName() {
        return name;
    }

    /**
     * Return the number of times this feature is used across the cluster.
     */
    public int getCount() {
        return count;
    }

    /**
     * Return the number of indices that use this feature across the cluster.
     */
    public int getIndexCount() {
        return indexCount;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IndexFeatureStats == false) {
            return false;
        }
        IndexFeatureStats that = (IndexFeatureStats) other;
        return name.equals(that.name) && count == that.count && indexCount == that.indexCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, count, indexCount);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("count", count);
        builder.field("index_count", indexCount);
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }
}
