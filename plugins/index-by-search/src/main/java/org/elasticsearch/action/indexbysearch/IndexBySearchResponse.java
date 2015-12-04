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

import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.INDEXED;
import static org.elasticsearch.action.indexbysearch.IndexBySearchResponse.Fields.TOOK;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

public class IndexBySearchResponse extends ActionResponse implements ToXContent {
    private long took;
    private long indexed;

    public IndexBySearchResponse() {
    }

    public IndexBySearchResponse(long took, long indexed) {
        this.took = took;
        this.indexed = indexed;
    }

    public long indexed() {
        return indexed;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        took = in.readVLong();
        indexed = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(took);
        out.writeVLong(indexed);
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString INDEXED = new XContentBuilderString("indexed");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK, took);
        builder.field(INDEXED, indexed);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexBySearchResponse[");
        builder.append("took=").append(took);
        builder.append(",indexed=").append(indexed);
        return builder.append("]").toString();
    }
}
