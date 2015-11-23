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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.IngestDocument;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.ingest.IngestDocument.MetaData.ID;
import static org.elasticsearch.ingest.IngestDocument.MetaData.INDEX;
import static org.elasticsearch.ingest.IngestDocument.MetaData.TYPE;

public class TransportData implements Writeable<TransportData>, ToXContent {

    private static final TransportData PROTOTYPE = new TransportData(null);

    private final IngestDocument ingestDocument;

    public TransportData(IngestDocument ingestDocument) {
        this.ingestDocument = ingestDocument;
    }

    public IngestDocument get() {
        return ingestDocument;
    }

    public static TransportData readTransportDataFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public TransportData readFrom(StreamInput in) throws IOException {
        String index = in.readString();
        String type = in.readString();
        String id = in.readString();
        Map<String, Object> doc = in.readMap();
        return new TransportData(new IngestDocument(index, type, id, doc));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ingestDocument.getMetadata(INDEX));
        out.writeString(ingestDocument.getMetadata(TYPE));
        out.writeString(ingestDocument.getMetadata(ID));
        out.writeMap(ingestDocument.getSource());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCUMENT);
        builder.field(Fields.MODIFIED, ingestDocument.isModified());
        builder.field(Fields.INDEX, ingestDocument.getMetadata(INDEX));
        builder.field(Fields.TYPE, ingestDocument.getMetadata(TYPE));
        builder.field(Fields.ID, ingestDocument.getMetadata(ID));
        builder.field(Fields.SOURCE, ingestDocument.getSource());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransportData that = (TransportData) o;
        return Objects.equals(ingestDocument, that.ingestDocument);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestDocument);
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
