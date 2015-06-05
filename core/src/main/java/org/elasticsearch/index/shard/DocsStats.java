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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class DocsStats implements Streamable, ToXContent {

    long count = 0;
    long deleted = 0;

    public DocsStats() {

    }

    public DocsStats(long count, long deleted) {
        this.count = count;
        this.deleted = deleted;
    }

    public void add(DocsStats docsStats) {
        if (docsStats == null) {
            return;
        }
        count += docsStats.count;
        deleted += docsStats.deleted;
    }

    public long getCount() {
        return this.count;
    }

    public long getDeleted() {
        return this.deleted;
    }

    public static DocsStats readDocStats(StreamInput in) throws IOException {
        DocsStats docsStats = new DocsStats();
        docsStats.readFrom(in);
        return docsStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
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
        static final XContentBuilderString DOCS = new XContentBuilderString("docs");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString DELETED = new XContentBuilderString("deleted");
    }
}
