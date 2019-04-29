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

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.MetaData;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

final class WriteableIngestDocument implements Writeable, ToXContentFragment {

    static final String SOURCE_FIELD = "_source";
    static final String INGEST_FIELD = "_ingest";
    static final String DOC_FIELD = "doc";
    private final IngestDocument ingestDocument;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<WriteableIngestDocument, Void> INGEST_DOC_PARSER =
        new ConstructingObjectParser<>(
            "ingest_document",
            true,
            a -> {
                HashMap<String, Object> sourceAndMetadata = new HashMap<>();
                sourceAndMetadata.put(MetaData.INDEX.getFieldName(), a[0]);
                sourceAndMetadata.put(MetaData.TYPE.getFieldName(), a[1]);
                sourceAndMetadata.put(MetaData.ID.getFieldName(), a[2]);
                if (a[3] != null) {
                    sourceAndMetadata.put(MetaData.ROUTING.getFieldName(), a[3]);
                }
                if (a[4] != null) {
                    sourceAndMetadata.put(MetaData.VERSION.getFieldName(), a[4]);
                }
                if (a[5] != null) {
                    sourceAndMetadata.put(MetaData.VERSION_TYPE.getFieldName(), a[5]);
                }
                sourceAndMetadata.putAll((Map<String, Object>)a[6]);
                return new WriteableIngestDocument(new IngestDocument(sourceAndMetadata, (Map<String, Object>)a[7]));
            }
        );
    static {
        INGEST_DOC_PARSER.declareString(constructorArg(), new ParseField(MetaData.INDEX.getFieldName()));
        INGEST_DOC_PARSER.declareString(constructorArg(), new ParseField(MetaData.TYPE.getFieldName()));
        INGEST_DOC_PARSER.declareString(constructorArg(), new ParseField(MetaData.ID.getFieldName()));
        INGEST_DOC_PARSER.declareString(optionalConstructorArg(), new ParseField(MetaData.ROUTING.getFieldName()));
        INGEST_DOC_PARSER.declareLong(optionalConstructorArg(), new ParseField(MetaData.VERSION.getFieldName()));
        INGEST_DOC_PARSER.declareString(optionalConstructorArg(), new ParseField(MetaData.VERSION_TYPE.getFieldName()));
        INGEST_DOC_PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField(SOURCE_FIELD));
        INGEST_DOC_PARSER.declareObject(
            constructorArg(),
            (p, c) -> {
                Map<String, Object> ingestMap = p.map();
                ingestMap.computeIfPresent(
                    "timestamp",
                    (k, o) -> ZonedDateTime.parse((String)o)
                );
                return ingestMap;
            },
            new ParseField(INGEST_FIELD)
        );
    }

    public static final ConstructingObjectParser<WriteableIngestDocument, Void> PARSER =
        new ConstructingObjectParser<>(
            "writeable_ingest_document",
            true,
            a -> (WriteableIngestDocument)a[0]
        );
    static {
        PARSER.declareObject(constructorArg(), INGEST_DOC_PARSER, new ParseField(DOC_FIELD));
    }

    WriteableIngestDocument(IngestDocument ingestDocument) {
        assert ingestDocument != null;
        this.ingestDocument = ingestDocument;
    }

    WriteableIngestDocument(StreamInput in) throws IOException {
        Map<String, Object> sourceAndMetadata = in.readMap();
        Map<String, Object> ingestMetadata = in.readMap();
        this.ingestDocument = new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(ingestDocument.getSourceAndMetadata());
        out.writeMap(ingestDocument.getIngestMetadata());
    }

    IngestDocument getIngestDocument() {
        return ingestDocument;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DOC_FIELD);
        Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.getMetadata();
        for (Map.Entry<IngestDocument.MetaData, Object> metadata : metadataMap.entrySet()) {
            if (metadata.getValue() != null) {
                builder.field(metadata.getKey().getFieldName(), metadata.getValue().toString());
            }
        }
        Map<String, Object> source = IngestDocument.deepCopyMap(ingestDocument.getSourceAndMetadata());
        metadataMap.keySet().forEach(mD -> source.remove(mD.getFieldName()));
        builder.field(SOURCE_FIELD, source);
        builder.field(INGEST_FIELD, ingestDocument.getIngestMetadata());
        builder.endObject();
        return builder;
    }

    public static WriteableIngestDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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
}
