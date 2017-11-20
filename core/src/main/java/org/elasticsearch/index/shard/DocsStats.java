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

package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.store.StoreStats;

import java.io.IOException;

public class DocsStats implements Streamable, ToXContentFragment {

    long count = 0;
    long deleted = 0;
    long totalSizeInBytes = 0;

    public DocsStats() {

    }

    public DocsStats(long count, long deleted, long totalSizeInBytes) {
        this.count = count;
        this.deleted = deleted;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public void add(DocsStats other) {
        if (other == null) {
            return;
        }
        this.totalSizeInBytes += other.totalSizeInBytes;
        this.count += other.count;
        this.deleted += other.deleted;
    }

    public long getCount() {
        return this.count;
    }

    public long getDeleted() {
        return this.deleted;
    }

    /**
     * Returns the total size in bytes of all documents in this stats.
     * This value may be more reliable than {@link StoreStats#getSizeInBytes()} in estimating the index size.
     */
    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    /**
     * Returns the average size in bytes of all documents in this stats.
     */
    public long getAverageSizeInBytes() {
        long totalDocs = count + deleted;
        return totalDocs == 0 ? 0 : totalSizeInBytes / totalDocs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            totalSizeInBytes = in.readVLong();
        } else {
            totalSizeInBytes = -1;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeVLong(totalSizeInBytes);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCS);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.DELETED, deleted);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String DOCS = "docs";
        static final String COUNT = "count";
        static final String DELETED = "deleted";
    }
}
