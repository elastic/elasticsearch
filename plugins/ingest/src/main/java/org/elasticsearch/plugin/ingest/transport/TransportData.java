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

package org.elasticsearch.plugin.ingest.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.Data;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class TransportData implements Streamable, ToXContent {
    private Data data;

    public TransportData() {

    }

    public TransportData(Data data) {
        this.data = data;
    }

    public Data get() {
        return data;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        String index = in.readString();
        String type = in.readString();
        String id = in.readString();
        Map<String, Object> doc = in.readMap();
        this.data = new Data(index, type, id, doc);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(data.getIndex());
        out.writeString(data.getType());
        out.writeString(data.getId());
        out.writeMap(data.getDocument());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCUMENT);
        builder.field(Fields.MODIFIED, data.isModified());
        builder.field(Fields.INDEX, data.getIndex());
        builder.field(Fields.TYPE, data.getType());
        builder.field(Fields.ID, data.getId());
        builder.field(Fields.SOURCE, data.getDocument());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransportData that = (TransportData) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENT = new XContentBuilderString("doc");
        static final XContentBuilderString MODIFIED = new XContentBuilderString("modified");
        static final XContentBuilderString INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ID = new XContentBuilderString("_id");
        static final XContentBuilderString SOURCE = new XContentBuilderString("_source");
    }
}
