/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Statistics about an index feature.
 */
public final class IndexFeatureStats implements ToXContent, Writeable {

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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("count", count);
        builder.field("index_count", indexCount);
        builder.endObject();
        return builder;
    }

}
