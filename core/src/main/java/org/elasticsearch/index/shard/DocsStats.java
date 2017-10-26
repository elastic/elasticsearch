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

import java.io.IOException;

public class DocsStats implements Streamable, ToXContentFragment {

    long count = 0;
    long deleted = 0;
    long averageSizeInBytes = 0;

    public DocsStats() {

    }

    public DocsStats(long count, long deleted, long averageSizeInBytes) {
        this.count = count;
        this.deleted = deleted;
        this.averageSizeInBytes = averageSizeInBytes;
    }

    public void add(DocsStats that) {
        if (that == null) {
            return;
        }
        long totalBytes = this.averageSizeInBytes * (this.count + this.deleted)
                        + that.averageSizeInBytes * (that.count + that.deleted);
        long totalDocs = this.count + this.deleted + that.count + that.deleted;
        if (totalDocs > 0) {
            this.averageSizeInBytes = totalBytes / totalDocs;
        }
        this.count += that.count;
        this.deleted += that.deleted;
    }

    public long getCount() {
        return this.count;
    }

    public long getDeleted() {
        return this.deleted;
    }

    public long getAverageSizeInBytes() {
        return averageSizeInBytes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            averageSizeInBytes = in.readVLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeVLong(averageSizeInBytes);
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
