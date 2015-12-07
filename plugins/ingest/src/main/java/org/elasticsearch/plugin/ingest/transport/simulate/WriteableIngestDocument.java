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

package org.elasticsearch.plugin.ingest.transport.simulate;

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

final class WriteableIngestDocument implements Writeable<WriteableIngestDocument>, ToXContent {

    private static final WriteableIngestDocument PROTOTYPE = new WriteableIngestDocument();

    private final IngestDocument ingestDocument;

    private WriteableIngestDocument() {
        this.ingestDocument = null;
    }

    WriteableIngestDocument(IngestDocument ingestDocument) {
        assert ingestDocument != null;
        this.ingestDocument = ingestDocument;
    }

    IngestDocument getIngestDocument() {
        return ingestDocument;
    }

    static WriteableIngestDocument readWriteableIngestDocumentFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    @Override
    public WriteableIngestDocument readFrom(StreamInput in) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, String> esMetadata = (Map<String, String>) in.readGenericValue();
        Map<String, Object> source = in.readMap();
        @SuppressWarnings("unchecked")
        Map<String, String> ingestMetadata = (Map<String, String>) in.readGenericValue();
        return new WriteableIngestDocument(new IngestDocument(esMetadata, source, ingestMetadata));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(ingestDocument.getEsMetadata());
        out.writeMap(ingestDocument.getSource());
        out.writeGenericValue(ingestDocument.getIngestMetadata());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCUMENT);
        builder.field(Fields.MODIFIED, ingestDocument.isSourceModified());
        for (Map.Entry<String, String> esMetadata : ingestDocument.getEsMetadata().entrySet()) {
            builder.field(esMetadata.getKey(), esMetadata.getValue());
        }
        builder.field(Fields.SOURCE, ingestDocument.getSource());
        builder.startObject(Fields.INGEST);
        for (Map.Entry<String, String> ingestMetadata : ingestDocument.getIngestMetadata().entrySet()) {
            builder.field(ingestMetadata.getKey(), ingestMetadata.getValue());
        }
        builder.endObject();
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
        WriteableIngestDocument that = (WriteableIngestDocument) o;
        return Objects.equals(ingestDocument, that.ingestDocument);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestDocument);
    }

    @Override
    public String toString() {
        return ingestDocument.toString();
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENT = new XContentBuilderString("doc");
        static final XContentBuilderString MODIFIED = new XContentBuilderString("modified");
        static final XContentBuilderString SOURCE = new XContentBuilderString("_source");
        static final XContentBuilderString INGEST = new XContentBuilderString("_ingest");
    }
}
