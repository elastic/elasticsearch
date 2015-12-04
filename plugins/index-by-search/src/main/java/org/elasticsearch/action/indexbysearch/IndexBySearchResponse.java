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

package org.elasticsearch.action.indexbysearch;

import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.CREATED;
import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.TOOK;
import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.UPDATED;
import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.VERSION_CONFLICTS;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

public class IndexBySearchResponse extends ActionResponse implements ToXContent {
    private long took;
    private long created;
    private long updated;
    private long versionConflicts;

    public IndexBySearchResponse() {
    }

    public IndexBySearchResponse(long took, long created, long updated, long versionConflicts) {
        this.took = took;
        this.created = created;
        this.updated = updated;
        this.versionConflicts = versionConflicts;
    }

    public long updated() {
        return updated;
    }

    public long created() {
        return created;
    }

    public long versionConflicts() {
        return versionConflicts;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = in.readVLong();
        updated = in.readVLong();
        created = in.readVLong();
        versionConflicts = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(took);
        out.writeVLong(updated);
        out.writeVLong(created);
        out.writeVLong(versionConflicts);
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
        static final XContentBuilderString UPDATED = new XContentBuilderString("updated");
        static final XContentBuilderString VERSION_CONFLICTS = new XContentBuilderString("versionConflicts");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK, took);
        builder.field(CREATED, created);
        builder.field(UPDATED, updated);
        builder.field(VERSION_CONFLICTS, versionConflicts);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexBySearchResponse[");
        builder.append("took=").append(took);
        builder.append(",created=").append(created);
        builder.append(",updated=").append(updated);
        builder.append(",versionConflicts=").append(versionConflicts);
        return builder.append("]").toString();
    }
}
